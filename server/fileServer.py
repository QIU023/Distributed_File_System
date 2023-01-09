# -*-coding:utf-8-*-
# 编辑者：QiuYQ, XuZH
import socket
import os
import re
import sys
import base64
import time
import grpc
import Distribute_pb2
import Distribute_pb2_grpc
from concurrent import futures


class File_Server(Distribute_pb2_grpc.File_ServerServicer):
    # 主机信息
    PORT = 8000
    HOST = '0.0.0.0'
    # 返回常量定义
    UPLOAD_RESPONSE = "OK: 0\n\n"
    DOWNLOAD_RESPONSE = "DATA: %s\n\n"
    # 文件存放地址？常量定义
    SERVER_ROOT = os.getcwd()
    BUCKET_NAME = "DirectoryServerFiles"
    BUCKET_LOCATION = os.path.join(SERVER_ROOT, BUCKET_NAME)
    DIR_HOST = "0.0.0.0"
    DIR_PORT = 8005         # master/proposer's port
    
    # 请求定义
    GET_SLAVES_HEADER = "GET_SLAVES: %s\nPORT: %s\n\n"
    UPDATE_HEADER = "UPDATE: %s\nDATA: %s\n\n"
    # paxos consistency message headers, for slaves / acceptor
    PROPOSER_PREPARE_REGEX = "PROPOSER_PREPARE_N: [0-9_.]*\n\n"
    PROPOSER_ACCEPT_REGEX = "PROPOSER_ACCEPT_N: [0-9_.]*\nPROPOSER_ACCEPT_V: .*\n\n"

    ACCEPTOR_POK_HEADER = "HOST: %s\nPORT: %s\nACCEPTOR_POK: %s\nACCEPTOR_ACCEPT_N: %d\n\n"
    ACCEPTOR_AOK_HEADER = "HOST: %s\nPORT: %s\nACCEPTOR_AOK: %s\n\n"
    SENDALL_DATA_TO_MASTER = "SENDALL_DATA_TO_MASTER\n\n"
    UPLOAD_HEADER = "UPLOAD: %s\tDATA: %s\n\n"
    RECVALL_DATA_FROM_CHOSEN_SLAVE_REGEX = "SENDALL_DATA_TO_ALL_SLAVES_HEADER\n[a-zA-Z0-9_.]*"

    SEND_SLAVE_ACCESS_STATUS_HEADER = "HOST: %s\nPORT: %s\nSLAVE_ACCESS_STATUS: %d\n\n"
    access_stat_interval = 100000


    def __init__(self):
        self.BUCKET_LOCATION = os.path.join(self.BUCKET_LOCATION, str(self.PORT))
        self.slave_accepted_timestamp = time.time()
        self.access_count = []

    def my_send_request(self, send_str, host, port):        
        if type(port) == str:
            port = int(port)
        with grpc.insecure_channel("{0}:{1}".format(host, port)) as channel:
            dir_cil=Distribute_pb2_grpc.File_ServerStub(channel=channel)
            return_str = dir_cil.send_bare_info(Distribute_pb2.file_request(message=send_str))
        return return_str

    def send_bare_info(self, request, context):
        return Distribute_pb2.file_reply(result=request.message)

    def modify_access_status(self):
        curr_time = time.time()
        self.access_count.append(curr_time)

        while (len(self.access_count) > 0 and \
            self.access_count[0] < curr_time - self.access_stat_interval):
            del self.access_count[0]

    def send_bare_info(self, request, context):
        return Distribute_pb2.file_reply(result=request.message)

    # 被调用的功能函数
    def upload_file(self, request, context):
        self.modify_access_status()
        # Handler for file upload requests
        filename, data = self.execute_write(request.message)
        return_string = self.UPLOAD_RESPONSE
        self.update_slaves(filename, data)
        return Distribute_pb2_grpc.file_reply(result=return_string)

    def download_file(self, request, context):
        self.modify_access_status()
        # Handler for file download requests
        request = request.message.splitlines()
        filename = request[0].split()[1]

        path = os.path.join(self.BUCKET_LOCATION, filename)
        file_handle = open(path, "w+")
        data = file_handle.read()
        return_string = self.DOWNLOAD_RESPONSE % (base64.b64encode(data))
        return Distribute_pb2_grpc.file_reply(result=return_string)

    def update_file(self, request, context):
        self.modify_access_status()
        # Handler for file update requests
        self.execute_write(request.message)
        return_string = self.UPLOAD_RESPONSE
        return Distribute_pb2_grpc.file_reply(result=return_string)

    def update_all(self, request, context):
        self.modify_access_status()
        for data_i in request.message.splitlines():
            data_i = data_i.split('\t')
            self.execute_write('\n'.join(data_i))
        # 不知道要不要补返回信息
        return

    def send_access_info(self, request, context):
        self.modify_access_status()
        return_string = self.SEND_SLAVE_ACCESS_STATUS_HEADER % (self.HOST, self.PORT, len(self.access_count))
        # self.send_request(return_string, self.DIR_HOST, self.DIR_PORT)
        return Distribute_pb2_grpc.file_reply(result=return_string) 
        # not sure if it's sent to directory server

    def paxos_accept_response(self, request, context):
        self.modify_access_status()
        # Stage 2
        # Acceptor: response to the accept from the proposer
        _request = request.message.splitlines()
        accept_timestamp = int(_request[0].split()[1])
        response_info = ""
        if accept_timestamp <= self.slave_accepted_timestamp:
            response_info = "error"
        else:
            self.slave_accepted_timestamp = accept_timestamp
            response_info = "AoK"
        response_str = self.ACCEPTOR_AOK_HEADER % response_info
        return Distribute_pb2_grpc.file_reply(result=response_str)

    def paxos_prepare_response(self, request, context):
        self.modify_access_status()
        # Stage 1
        # Acceptor: response to the prepare from the proposer
        _request = request.splitlines()
        prepare_timestamp = int(_request[0].split()[1])
        response_info = ""
        if prepare_timestamp <= self.slave_accepted_timestamp:
            response_info = "error"
            response_str = self.ACCEPTOR_POK_HEADER % (
                self.HOST, str(self.PORT), response_info, "null")
        else:
            self.slave_accepted_timestamp = prepare_timestamp
            slave_cur_timestamp = time.time()
            response_info = "PoK"
            response_str = self.ACCEPTOR_POK_HEADER % (
                self.HOST, str(self.PORT), response_info, slave_cur_timestamp)
        return Distribute_pb2_grpc.file_reply(result=response_str)

    # 有Dir请求
    def paxos_send_acceptV(self, request, context):
        self.modify_access_status()

        all_files_info = os.listdir(self.BUCKET_LOCATION)
        all_files_str = ""
        acceptor_cur_timestamp = time.time()
        for filename in all_files_info:
            filepath = os.path.join(self.BUCKET_LOCATION, filename)
            file_handle = open(filepath, 'r')
            data = file_handle.read()
            all_files_str += self.UPLOAD_HEADER % (filename, base64.b64encode(data))
        with grpc.insecure_channel("{0}:{1}".format(self.HOST, self.PORT)) as channel:
            dir_cil=Distribute_pb2_grpc.Direct_ServerStub(channel=channel)
            response=dir_cil.paxos_update_response_status(Distribute_pb2.dir_request(message=all_files_str))
        return Distribute_pb2_grpc.file_reply(result=all_files_str) 
        # self.send_request(all_files_str, self.DIR_HOST, self.DIR_PORT)

    # 底层函数
    def execute_write(self, text):
        # Function that process an update/upload request and writes data to the server
        request = text.splitlines()
        filename = request[0].split()[1]
        data = request[1].split()[1]
        data = base64.b64decode(data)

        path = os.path.join(self.BUCKET_LOCATION, filename)
        file_handle = open(path, "w+")
        file_handle.write(data)
        return filename, data

    # 有Dir请求
    def get_slaves(self):
        # Function that operate the slave to get the list of slaves (including itself) from host
        return_list = []
        # 这里需要向dir获取目录，待会修改，同样需要
        request_data = self.GET_SLAVES_HEADER % (self.HOST, self.PORT)
        lines = self.my_send_request(request_data, self.DIR_HOST, self.DIR_PORT).splitlines()

        slaves = lines[1:-1]
        for i in range(0, len(slaves), 2):
            host = slaves[i].split()[1]
            port = slaves[i + 1].split()[1]
            return_list.append((host, port))
        return return_list

    # 有Dir请求
    def update_slaves(self, filename, data):
        # Function that gets all the slaves and updates file on them
        slaves = self.get_slaves()
        update = self.UPDATE_HEADER % (filename, base64.b64encode(data))
        for (host, port) in slaves:
            self.my_send_request(update, host, port)
        return


def main():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 实例化 计算len的类
    servicer = File_Server()
    # 注册本地服务,方法ComputeServicer只有这个是变的
    Distribute_pb2_grpc.add_File_ServerServicer_to_server(servicer, server)
    # compute_pb2_grpc.add_ComputeServicer_to_server(servicer, server)
    # 监听端口
    server.add_insecure_port('127.0.0.1:19999')
    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    try:
        print("running...")
        time.sleep(1000)
    except KeyboardInterrupt:
        print("stopping...")
        server.stop(0)


if __name__ == "__main__":
    main()
