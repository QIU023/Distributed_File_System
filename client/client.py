# 客户端 作为与用户的交互
import socket
import sys
import os
import re
import threading
import time
import queue
import base64

from copy import deepcopy, copy
import numpy as np

from LRU_cache import TwoQueue_Cache

# 与服务器交流用的指令的正则表达式
UPLOAD_REGEX = "upload [a-zA-Z0-9_]*."
DOWNLOAD_REGEX = "download [a-zA-Z0-9_]*."
DIRECTORY_REGEX = "dir [a-zA-Z0-9_/.]*"
LOCK_REGEX = "lock [a-zA-Z0-9_/.]* [0-9]*"

import redis

# TCP链接
class TCPClient:
    PORT = 8000
    HOST = "0.0.0.0"
    DIR_PORT = 8005
    FILE_PORT = 8006
    LOCK_PORT = 8007

    # redis 缓存的参数：端口号、密码、设置的大小（64MB）
    LOCAL_REDIS_PORT = 8008
    LOCAL_REDIS_PASSWD = "aaaaaaaa"
    LOCAL_CACHE_LIMIT = 2**24

    # 本地文件加锁次数上限
    LOCK_TRY_MAX_TIME = 10
    LOCK_TYPE_CONFLICT = "LOCK TYPE CONFLICT!!!"

    DIR_HOST = HOST
    LOCK_HOST = HOST
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

    # path = '\\'.join(CLIENT_ROOT)
    # BUCKET_NAME = "ClientFiles"
    # BUCKET_LOCATION = os.path.join(CLIENT_ROOT, BUCKET_NAME)
    GET_SLAVE_ACCESS_STATUS_HEADER = "GET_SLAVE_ACCESS_STATUS\n\n"
    RECV_SLAVE_ACCESS_STATUS_REGEX = "SLAVE_ACCESS_STATUS_TO_CLIENT\n[a-zA-Z0-9_.]*\n\n"
    # RECV_SLAVE_ACCESS_STATUS_REGEX = "HOST: [a-zA-Z0-9_.]*\tPORT: [0-9_.]*\tSLAVE_ACCESS_STATUS: [a-zA-Z0-9_.]*\t"
    ANNOUNCE_OPTIMAL_SLAVE = "CLIENT_HOST: %s\nCLIENT_PORT: %s\nOPTIMAL_SLAVE_HOST: %s\nOPTIMAL_SLAVE_PORT: %s\n\n"

    def __init__(self, port_use=None):
        if not port_use:
            self.port_use = self.PORT
        else:
            self.port_use = port_use
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.open_files = {}
        self.cache_info = TwoQueue_Cache()
        self.cache_used_size = 0
        self.threadQueue = queue.Queue()

        self.weight_numhoop = 1
        self.weight_time = 1
        self.weight_access = 1

        self.slave_access_info = {}
        self.local_redis_conn = redis.Redis(host = '127.0.0.1', port = self.LOCAL_REDIS_PORT, password=self.LOCAL_REDIS_PASSWD, db = 0)

    def get_file_info(self, file_istream):
        return {
            'access_time': 1,
            'create_time_stamp': time.time(),
            'access_time_stamp': time.time(),
            'size': file_istream.__sizeof__() # bytes ->
        }

    def write_cache(self, filename, data):
        pop_name = self.cache_info.put(filename,self.get_file_info(data))
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
                self.local_redis_conn.getset(filename,data)


    # 为了对应缓存，因此需要修改，先访问缓存
    def open(self, filename, access_type='read'):
        # 缓存机制
        """Function opens a file by downloading from a remote server"""
        file_downloaded = False
        if filename not in self.open_files.keys():
            #这里补充：不在缓存才去下载
            if filename not in self.cache_info.keys():
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
            else:
                file_downloaded = True
        else:
            file_downloaded = True
            # print("file already opened!")  
                
        return file_downloaded

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
                        '''
                        不删，用作缓存
                        if os.path.exists(path):
                            os.remove(path)
                        '''
                        del self.open_files[filename]
                        self.__unlock_file(filename, access_type)
                # else:
                    # print("lock type error")
            # else:
                # print("server error!")
                
        return file_uploaded

    def read(self, filename):
        """Function that reads from an open file"""
        if filename in self.open_files.keys():
            local_name = self.open_files[filename]
            path = os.path.join(self.BUCKET_LOCATION, local_name)
            file_handle = open(path, "rb")
            data = file_handle.read()
            return data
        return None

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

    
    def client_get_optimal_slave(self, all_slave_hosts):
        # 流量管理算法/负载均衡算法
        # 获得所有slaves与client的ping延时？ 

        res_arr = []
        # remain = len(all_slave_hosts)
        for slave_host in all_slave_hosts:
            host, port = slave_host
            return_str = os.popen('tracert {}'.format(host)).read().splitlines()[4:-2]
            num_hoops = len(return_str)
            
            return_str = os.popen('ping {}'.format(host)).read()
            arr = return_str.splitlines()[-1].split('，')
            # min_time = arr[0].split('=')[1][1:-2]
            # max_time = arr[1].split('=')[1][1:-2]
            avg_time = arr[2].split('=')[1][1:-2]

            if (host, port) not in self.slave_access_info:
                slave_status_str = self.__send_request(self.GET_SLAVE_ACCESS_STATUS_HEADER, self.DIR_HOST, self.DIR_PORT)
                slave_status_str = slave_status_str.splitlines()[1:-1]
                for si in slave_status_str:
                    si = si.split('\t')
                    hi, pi, status_i = si
                    self.slave_access_info[(hi, pi)] = status_i
                
            access_info = self.slave_access_info[(host, port)]
            res_arr.append([num_hoops, avg_time, access_info, (host, port)])
        total_rank_arr = {}
        res_arr = np.array(res_arr)
        num_hoops_arr = np.argsort(res_arr[:, 0])
        time_arr = np.argsort(res_arr[:, 1])
        access_arr = np.argsort(res_arr[:, 2])

        for i in range(self.num_slaves):
            total_rank_arr[all_slave_hosts[num_hoops_arr[i]]] += i*self.weight_numhoop
            total_rank_arr[all_slave_hosts[time_arr[i]]] += i*self.weight_time
            total_rank_arr[all_slave_hosts[access_arr[i]]] += i*self.weight_access
        optimal_slave = min(total_rank_arr)
        send_str = self.ANNOUNCE_OPTIMAL_SLAVE % (self.HOST, self.PORT, optimal_slave[0], optimal_slave[1])
        self.__send_request(send_str, self.DIR_HOST, self.DIR_PORT)
        # return optimal_slave


    def __send_request(self, data, server, port):
        """Function that sends requests to remote server"""
        return_data = ""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.connect((server, port))
        sock.sendall(self.REQUEST % data)

        # Loop until all data received
        while "\n\n" not in return_data:
            data = sock.recv(self.LENGTH)
            if len(data) == 0:
                break
            return_data += data

        # Close and dereference the socket
        sock.close()
        sock = None
        return return_data

    def __raw_request(self, string):
        """Send a raw request to remote server"""
        return_data = ""
        # Do nothing if the string is empty or socket doesn't exist
        if len(string) > 0:
            # Create socket if it doesn't exist
            return_data = self.__send_request(string + "\n\n")
        return return_data

    def __upload_file(self, server, filename):
        """Send a request to the server to upload a file"""
        path = os.path.join(self.BUCKET_LOCATION, filename)

        file_handle = open(path, "rb")
        # Base64 encode the file so it can be sent in a message
        data = file_handle.read()
        data = base64.b64encode(data)

        request = self.UPLOAD_HEADER % (filename, data)

        return self.__send_request(request, server, self.FILE_PORT)


    # 下载文件 考虑同名覆盖的问题
    def __download_file(self, server, port, filename):
        """Send a request to the server to download a file"""
        path = os.path.join(self.BUCKET_LOCATION, filename)
        # Download message containing file data and then base64 decode the data
        request = self.DOWNLOAD_HEADER % (filename)
        request_data = self.__send_request(request, server, port).splitlines()[0]
        data = request_data.split()[0]

        data = base64.b64decode(data)
        file_handle = open(path, "wb+")
        file_handle.write(data)
        return True, data

    # 获取文件位置
    def __get_directory(self, filename):
        """Send a request to the server to find the location of a directory"""
        request = self.DIRECTORY_HEADER % (self.HOST, self.PORT, filename)
        # depend for the optimal slave server
        
        return self.__send_request(request, self.DIR_HOST, self.DIR_PORT)

    # 文件上锁
    def __lock_file(self, filename, lock_type, lock_time):
        """Send a request to the server to locks a file"""
        request = self.LOCK_HEADER % (filename, lock_type, lock_time)
        lock_try_time = 0

        request_data = self.__send_request(request, self.LOCK_HOST, self.LOCK_PORT)
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
                request_data = self.__send_request(request, self.LOCK_HOST, self.LOCK_PORT)
                lock_try_time += 1
        
            return not re.match(self.FAIL_RESPONSE, request_data)

    # 解锁
    def __unlock_file(self, filename, lock_type):
        """Send a request to the server to unlock a file"""
        request = self.UNLOCK_HEADER % (filename, lock_type)
        return self.__send_request(request, self.LOCK_HOST, self.LOCK_PORT)

    # 判断文件名是否已经重名
    def Is_in(self,filename):
        if not self.__get_directory(filename):
            return False
        return True

    # 创建文件
    def __create_file(self,filename):
        """创建空文件并上传至Server或者让Server创建同名文件"""
        if not self.Is_in(filename):
            full_path = '\\'.join(self.BUCKET_LOCATION)
            newfile = open(full_path, 'w')
            newfile.close()
            #这里应该有点问题
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
            self.__upload_file(server,filename)
            '''
            return True
        return False