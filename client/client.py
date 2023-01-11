# -*-coding:utf-8-*-
# 编辑者：XuZH
import sys
import os
import re
import threading
import time
import queue
import base64
import redis
import grpc
import Distribute_pb2
import Distribute_pb2_grpc
from LRU_cache import TwoQueue_Cache



class Client:
    #本主机号码和端口
    PORT = 8000
    HOST = "192.168.90.100"

    # redis 缓存的参数：端口号、密码、设置的大小（64MB）
    LOCAL_REDIS_PORT = 8008
    LOCAL_REDIS_PASSWD = "aaaaaaaa"
    LOCAL_CACHE_LIMIT = 2 ** 24

    # 本地文件加锁次数上限
    LOCK_TRY_MAX_TIME = 10
    LOCK_TYPE_CONFLICT = "LOCK TYPE CONFLICT!!!"

    # 将请求先设置为常量
    UPLOAD_HEADER = "UPLOAD: %s\nDATA: %s\n\n"
    DOWNLOAD_HEADER = "DOWNLOAD: %s\n\n"
    DIRECTORY_HEADER = "GET_SERVER: \nCLIENT_HOST: %s\nCLIENT_PORT: %s\nFILENAME: %s\n\n"
    SERVER_RESPONSE = "PRIMARY_SERVER: .*\nPORT: .*\nFILENAME: .*"
    LOCK_HEADER = "LOCK_FILE: %s\nLOCK_TYPE: %s\nTime: %d\n\n"
    LOCK_RESPONSE = "LOCK_RESPONSE: \nFILENAME: .*\nTIME: .*\n\n"
    FAIL_RESPONSE = "ERROR: .*\nMESSAGE: .*\n\n"
    UNLOCK_HEADER = "UNLOCK_FILE: %s\nUNLOCK_TYPE: %s\n\n"

    REQUEST = "%s"
    LENGTH = 4096

    # 下面三行 是文件存储位置 与参考资料代码数据放一起不同，选择放另一个目录下
    CLIENT_ROOT = os.getcwd()
    CLIENT_ROOT = CLIENT_ROOT.split('\\')
    CLIENT_ROOT[len(CLIENT_ROOT) - 1] = 'Client_files'
    BUCKET_LOCATION = '\\'.join(CLIENT_ROOT)

    def __init__(self):
        self.open_files = {}
        self.cache_info = TwoQueue_Cache(10)
        self.cache_used_size = 0
        self.threadQueue = queue.Queue()

    # 功能函数定义：

    # 文件IO和cache功能
    def get_file_info(self, file_istream):
        return {
            'access_time': 1,
            'create_time_stamp': time.time(),
            'access_time_stamp': time.time(),
            'size': file_istream.__sizeof__()  # bytes ->
        }

    def write_cache(self, filename, data):
        pop_name = self.cache_info.put(filename, self.get_file_info(data))
        self.update_redis_cache(pop_name, filename, data, is_read=False)

    def read_cache(self, filename):
        data = self.local_redis_conn.get(filename)
        pop_name = self.cache_info.get(filename)
        self.update_redis_cache(pop_name, filename, data, is_read=True)

        return data

    def update_redis_cache(self, pop_name, filename, data, is_read=False):
        if pop_name is not None:
            # cache 淘汰旧值，插入新值，适用于r/w
            self.local_redis_conn.delete(pop_name)
            self.local_redis_conn.set(filename, data)
        else:
            # cache 更新已有值, 适用于write
            if not is_read:
                self.local_redis_conn.getset(filename, data)

    # 打开文件
    def open(self, filename, access_type='read'):
        # 缓存机制
        """Function opens a file by downloading from a remote server"""
        file_downloaded = False
        if filename not in self.open_files.keys():
            # 这里补充：不在缓存才去下载
            if self.cache_info.is_in_cache(filename):
                # Get the info of the server hosting the file,
                # 涉及server分配策略！cdn负载均衡算法
                request = self.__get_directory(filename)
                if re.match(self.SERVER_RESPONSE, request):
                    params = request.splitlines()
                    server = params[0].split()[1]
                    port = int(params[1].split()[1])
                    open_file = params[2].split()[1]
                    # Get lock on file before downloading
                    self.__lock_file(filename, access_type, 10)
                    file_downloaded, data = self.__download_file(server, port, open_file)
                    if file_downloaded:
                        # print("getting file from the server!")
                        self.open_files[filename] = open_file
                        self.write_cache(filename, data)
                    # return file_downloaded
                else:
                    file_downloaded = False
                    # print("server connection error!")
            else:
                file_downloaded = True
                # print("file found in local cache!")
                # data = self.read_cache(filename)
            # ????
        else:
            file_downloaded = True
            # print("file already opened!")
        return file_downloaded

    # 关文件
    def close(self, filename, access_type='read'):
        """Function closes a file by uploading it, update cache and removing the local opening"""
        file_uploaded = False
        if filename in self.open_files.keys():
            request = self.__get_directory(filename)
            if re.match(self.SERVER_RESPONSE, request):
                # Remove lock from file
                if access_type == 'read':
                    self.__unlock_file(filename, access_type)
                elif access_type == 'write':
                    params = request.splitlines()
                    server = params[0].split()[1]
                    open_file = params[2].split()[1]
                    # Upload the file and
                    file_uploaded = self.__upload_file(server, open_file)
                    if file_uploaded:
                        # path = os.path.join(self.CLIENT_ROOT, self.BUCKET_NAME)
                        # path = os.path.join(path, self.open_files[filename])

                        data = self.read(filename)
                        self.write_cache(filename, data)
                        del self.open_files[filename]
                        self.__unlock_file(filename, access_type)
                # else:
                # print("lock type error")
            # else:
            # print("server error!")

        return file_uploaded

    # 读文件
    def read(self, filename):
        """Function that reads from an open file"""
        if filename in self.open_files.keys():
            local_name = self.open_files[filename]
            path = os.path.join(self.BUCKET_LOCATION, local_name)
            file_handle = open(path, "rb")
            data = file_handle.read()
            return data
        return None

    # 写文件
    def write(self, filename, data):
        """Function that writes to an open file"""
        success = False
        if filename in self.open_files.keys():
            local_name = self.open_files[filename]
            path = os.path.join(self.BUCKET_LOCATION, local_name)
            file_handle = open(path, "wb+")
            file_handle.write(data)
            success = True
        return success

    # 上传
    def __upload_file(self, filename):
        """Send a request to the server to upload a file"""
        path = os.path.join(self.BUCKET_LOCATION, filename)

        file_handle = open(path, "rb")
        # Base64 encode the file so it can be sent in a message
        data = file_handle.read()
        data = base64.b64encode(data)

        request = self.UPLOAD_HEADER % (filename, data)
        # 调用fileserver的upload
        with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
            dir_cil = Distribute_pb2_grpc.File_ServerStub(channel=channel)
            response = dir_cil.upload_file(Distribute_pb2.file_request(message=request))
        return response

    # 下载文件 考虑同名覆盖的问题
    def __download_file(self, filename):
        """Send a request to the server to download a file"""
        path = os.path.join(self.BUCKET_LOCATION, filename)
        # Download message containing file data and then base64 decode the data
        request = self.DOWNLOAD_HEADER % (filename)
        # 调用download
        with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
            dir_cil = Distribute_pb2_grpc.File_ServerStub(channel=channel)
            response = dir_cil.download_file(Distribute_pb2.file_request(message=request))
        request_data = response
        data = request_data.split()[0]

        data = base64.b64decode(data)
        file_handle = open(path, "wb+")
        file_handle.write(data)
        return True, data

    # 获取服务器
    def __get_directory(self, filename):
        """Send a request to the server to find the location of a directory"""
        request = self.DIRECTORY_HEADER % (self.HOST, self.PORT, filename)
        # 调用getserver
        with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
            dir_cil = Distribute_pb2_grpc.Direct_ServerStub(channel=channel)
            response = dir_cil.get_server(Distribute_pb2.dir_request(message=request))
        return response
        # self.__send_request(request, self.DIR_HOST, self.DIR_PORT)

    # 上锁
    def __lock_file(self, filename, lock_type, lock_time):
        """Send a request to the server to locks a file"""
        request = self.LOCK_HEADER % (filename, lock_type, lock_time)
        lock_try_time = 0
        # 调用lock
        with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
            dir_cil = Distribute_pb2_grpc.Lock_ServerStub(channel=channel)
            response = dir_cil.get_lock(Distribute_pb2.lock_Request(message=request))
        # request_data = self.__send_request(request, self.LOCK_HOST, self.LOCK_PORT)
        request_data = response
        if re.match(self.LOCK_TYPE_CONFLICT, request_data):
            # print(self.LOCK_TYPE_CONFLICT)
            return False

        else:
            while re.match(self.FAIL_RESPONSE, request_data) and lock_try_time < self.LOCK_TRY_MAX_TIME:
                # If failed to lock the file, wait a time and try again
                # cannot recursive calling like this !!!
                request_data = request_data.splitlines()
                wait_time = float(request_data[1].split()[1])
                time.sleep(wait_time)
                with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
                    dir_cil = Distribute_pb2_grpc.Lock_ServerStub(channel=channel)
                    response = dir_cil.get_lock(Distribute_pb2.lock_Request(message=request))
                request_data = response
                lock_try_time += 1

            return not re.match(self.FAIL_RESPONSE, request_data)

    # 解锁
    def __unlock_file(self, filename, lock_type):
        """Send a request to the server to unlock a file"""
        request = self.UNLOCK_HEADER % (filename, lock_type)
        with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
            dir_cil = Distribute_pb2_grpc.Lock_ServerStub(channel=channel)
            response = dir_cil.get_unlock(Distribute_pb2.lock_Request(message=request))
        return response

    # 判断文件名是否已经重名
    def Is_in(self, filename):
        if not self.__get_directory(filename):
            return False
        return True

    # 创建文件（不完善）
    def create_file(self, filename):
        """创建空文件并让Server创建同名文件"""
        if not self.Is_in(filename):
            full_path = '\\'.join(self.BUCKET_LOCATION)
            newfile = open(full_path, 'w')
            newfile.close()
            #有个情况，都是一句filename来找的，如果
            '''
            request = self.__get_directory(filename)
            if re.match(self.SERVER_RESPONSE, request):
                # Remove lock from file
                self.__unlock_file(filename)
                params = request.splitlines()
                server = params[0].split()[1]
                open_file = params[2].split()[1]
                # Upload the file and
                file_uploaded = self.__upload_file(server, open_file)
            '''
            self.__upload_file(filename)
            return True
        return False

    #删除文件
    def delete_file(self,filename):
        pass

    # 列出文件
    def list_files(self):
        return os.listdir(self.BUCKET_LOCATION)


# 需要写客户端main的
def main():
    client = Client()
    while True:
        choose = input("请输入需要服务的类型（数字）：1.获取客户端文件列表 2.read文件 3.write文件 \n 4.create文件  5.退出\n")
        choose = int(choose)
        if choose == 1:
            print(client.list_files())
        elif choose == 2:
            filename = input("请输入文件名：（确保服务器中有）")
            data = client.read(filename)
            print(data)
        elif choose == 3:
            filename = input("请输入文件名：（确保服务器中有）")
            data = input("请输入写入文件的信息：")
            client.write(filename, data)
        elif choose == 4:
            filename = input("请输入文件名：")
            client.create_file(filename)
        else:
            break


if __name__ == '__main__':
    main()
