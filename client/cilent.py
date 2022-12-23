# 客户端 作为与用户的交互
import socket
import sys
import os
import re
import threading
import time
import queue
import base64

# 与服务器交流用的指令的正则表达式
UPLOAD_REGEX = "upload [a-zA-Z0-9_]*."
DOWNLOAD_REGEX = "download [a-zA-Z0-9_]*."
DIRECTORY_REGEX = "dir [a-zA-Z0-9_/.]*"
LOCK_REGEX = "lock [a-zA-Z0-9_/.]* [0-9]*"


# TCP链接
class TCPClient:
    PORT = 8000
    HOST = "0.0.0.0"
    DIR_PORT = 8005
    FILE_PORT = 8006
    LOCK_PORT = 8007
    DIR_HOST = HOST
    LOCK_HOST = HOST
    # 将请求先设置为常量
    UPLOAD_HEADER = "UPLOAD: %s\nDATA: %s\n\n"
    DOWNLOAD_HEADER = "DOWNLOAD: %s\n\n"
    DIRECTORY_HEADER = "GET_SERVER: \nFILENAME: %s\n\n"
    SERVER_RESPONSE = "PRIMARY_SERVER: .*\nPORT: .*\nFILENAME: .*"
    LOCK_HEADER = "LOCK_FILE: %s\nTime: %d\n\n"
    LOCK_RESPONSE = "LOCK_RESPONSE: \nFILENAME: .*\nTIME: .*\n\n"
    FAIL_RESPONSE = "ERROR: .*\nMESSAGE: .*\n\n"
    UNLOCK_HEADER = "UNLOCK_FILE: %s\n\n"
    REQUEST = "%s"
    LENGTH = 4096
    # 下面三行 是文件存储位置 与参考资料代码数据放一起不同，选择放另一个目录下
    CLIENT_ROOT = os.getcwd()
    CLIENT_ROOT = CLIENT_ROOT.split('\\')
    CLIENT_ROOT[len(CLIENT_ROOT) - 1] = 'Cilent_files'
    BUCKET_LOCATION = '\\'.join(CLIENT_ROOT)

    # path = '\\'.join(CLIENT_ROOT)
    # BUCKET_NAME = "ClientFiles"
    # BUCKET_LOCATION = os.path.join(CLIENT_ROOT, BUCKET_NAME)

    def __init__(self, port_use=None):
        if not port_use:
            self.port_use = self.PORT
        else:
            self.port_use = port_use
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.open_files = {}
        self.cache={}
        self.threadQueue = queue.Queue()

    # 为了对应缓存，因此需要修改，先访问缓存
    def open(self, filename):
        # 缓存机制
        """Function opens a file by downloading from a remote server"""
        file_downloaded = False
        if filename not in self.open_files.keys():
            #这里补充：不在缓存才去下载
            if filename not in self.cache.keys():
                # Get the info of the server hosting the file
                request = self.__get_directory(filename)
                if re.match(self.SERVER_RESPONSE, request):
                    params = request.splitlines()
                    server = params[0].split()[1]
                    port = int(params[1].split()[1])
                    open_file = params[2].split()[1]
                    # Get lock on file before downloading
                    self.__lock_file(filename, 10)
                    file_downloaded = self.__download_file(server, port, open_file)
                    if file_downloaded:
                        self.open_files[filename] = open_file
                        self.cache[filename]=open_file
        return file_downloaded

    def close(self, filename):
        """Function closes a file by uploading it and removing the local copy"""
        file_uploaded = False
        if filename in self.open_files.keys():
            request = self.__get_directory(filename)
            if re.match(self.SERVER_RESPONSE, request):
                # Remove lock from file
                self.__unlock_file(filename)
                params = request.splitlines()
                server = params[0].split()[1]
                open_file = params[2].split()[1]
                # Upload the file and
                file_uploaded = self.__upload_file(server, open_file)
                if file_uploaded:
                    path = os.path.join(self.CLIENT_ROOT, self.BUCKET_NAME)
                    path = os.path.join(path, self.open_files[filename])
                    '''
                    不删，用作缓存
                    if os.path.exists(path):
                        os.remove(path)
                    '''
                    del self.open_files[filename]
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
        return True

    # 获取文件位置
    def __get_directory(self, filename):
        """Send a request to the server to find the location of a directory"""
        request = self.DIRECTORY_HEADER % filename

        return self.__send_request(request, self.DIR_HOST, self.DIR_PORT)

    # 文件上锁
    def __lock_file(self, filename, lock_time):
        """Send a request to the server to locks a file"""
        request = self.LOCK_HEADER % (filename, lock_time)
        request_data = self.__send_request(request, self.LOCK_HOST, self.LOCK_PORT)
        if re.match(self.FAIL_RESPONSE, request_data):
            # If failed to lock the file, wait a time and try again
            request_data = request_data.splitlines()
            wait_time = float(request_data[1].split()[1])
            time.sleep(wait_time)
            self.__lock_file(filename, lock_time)
        return True

    # 解锁
    def __unlock_file(self, filename):
        """Send a request to the server to unlock a file"""
        request = self.UNLOCK_HEADER % filename
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