# -*-coding:utf-8-*-
# 编辑者：QiuYQ, XuZH
import grpc
import Distribute_pb2
import Distribute_pb2_grpc
from concurrent import futures
import socket
import re
import sys
import os
import hashlib
import random
import sqlite3 as db
import time
from collections import OrderedDict


class Direct_Server(Distribute_pb2_grpc.Direct_ServerServicer):
    # 返回信息常量定义
    GET_RESPONSE = "PRIMARY_SERVER: %s\nPORT: %s\nFILENAME: %s%s\n\n"
    SLAVE_RESPONSE_HEADER = "SLAVES: %s\n\n"
    SLAVE_HEADER = "\nSLAVE_SERVER: %s\nPORT: %s"
    GETALL_DATA_FROM_A_SLAVE = "SENDALL_DATA_TO_MASTER\n\n"
    SENDALL_DATA_TO_ALL_SLAVES_HEADER = "SENDALL_DATA_TO_ALL_SLAVES_HEADER\n\n%s"

    DATABASE = "Database/directories.db"

    DIR_HOST = "0.0.0.0"
    DIR_PORT = 8005  # master/proposer's port
    CHECK_INTERVAL = 60

    # paxos consistency message headers, for master / proposer
    PAXOS_CHECK_REGEX = "PAXOS_CHECK\n\n"
    PROPOSER_PREPARE_HEADER = "PROPOSER_PREPARE_N: %s\n\n"
    PROPOSER_ACCEPT_HEADER = "PROPOSER_ACCEPT_N: %s\nPROPOSER_ACCEPT_V: %s\n\n"
    ACCEPTOR_POK_REGEX = "HOST: [a-zA-Z0-9_.]*\n\PORT: [0-9_.]*\nACCEPTOR_POK: [a-zA-Z0-9_.]*\nACCEPTOR_ACCEPT_N: [0-9_.]*\n\nACCEPTOR_ACCEPT_V: .*\n\n"
    ACCEPTOR_AOK_REGEX = "HOST: [a-zA-Z0-9_.]*\nPORT: [0-9_.]*\nACCEPTOR_AOK: [a-zA-Z0-9_.]*\n"
    SENDALL_DATA_REGEX = "ALL_DATA_TO_MASTER\nACCEPT_N: [0-9_.]\nALLDATA: [a-zA-Z0-9_.]*\n\n"

    # Load balance/Traffic Management Algorithm for slaves
    GET_SLAVE_ACCESS_STATUS_HEADER = "GET_SLAVE_ACCESS_STATUS\n\n"
    STRATEGY_ = "Load Balancing and Traffic Management"
    RECV_SLAVE_ACCESS_STATUS_REGEX = "SLAVE_ACCESS_STATUS: [a-zA-Z0-9_./]*\n\n"
    SEND_SLAVE_ACCESS_STATUS_HEADER = "HOST: %s\tPORT: %s\tSLAVE_ACCESS_STATUS: %d\n"

    def __init__(self):
        # create tables不知道是什么
        self.create_tables()
        self.slave_nodes = []

        self.paxos_slave_acceptN_dict = {(self.DIR_HOST, self.DIR_PORT): time.time()}
        self.num_slaves = 0
        self.pok_sum = [0, 0]
        self.aok_sum = [0, 0]
        self.chosen_slave = None
        self.paxos_trying_times_limit = 5
        self.paxos_trying_times = 0
        self.paxos_check_interval = 1000000
        self.paxos_previous_check_time = time.time()
        self.paxos_cur_stage = 0

        self.client2slaves_access_info = OrderedDict()
        self.slave_access_info = {}
        self.temp_filename_request_dict = {}

    def my_send_request(self, send_str, host, port):        
        if type(port) == str:
            port = int(port)
        with grpc.insecure_channel("{0}:{1}".format(host, port)) as channel:
            dir_cil=Distribute_pb2_grpc.Direct_ServerStub(channel=channel)
            dir_cil.send_bare_info(Distribute_pb2.dir_request(message=send_str))

    def send_bare_info(self, request, context):
        return Distribute_pb2.dir_reply(result=request.message)

    # 功能函数定义
    def get_server(self, request, context):
        # Handler for file upload requests
        _request = request.message.splitlines()
        client_host = _request[1]
        client_port = int(_request[2])
        full_path = _request[3].split()[1]

        path, file = os.path.split(full_path)
        name, ext = os.path.splitext(file)
        filename = hashlib.sha256(full_path).hexdigest() + ext
        all_slave_hosts = self.find_host(path)
        host, port = self.slave_fileserver_distribute(all_slave_hosts, 
            client_host, client_port, strategy=self.STRATEGY_)

        if self.STRATEGY_ != "Load Balancing and Traffic Management":
            if not host:
                # The Directory doesn't exist and must be added to the db
                server_id = self.pick_random_host()
                self.create_dir(path, server_id)
                all_slave_hosts = self.find_host(path)
                host, port = self.slave_fileserver_distribute(all_slave_hosts, client_host, client_port, strategy='random')

            # Get the list of slaves that have a copy of the file
            slave_string = self.get_slave_string(host, port)
            return_string = self.GET_RESPONSE % (host, port, filename, slave_string)
        else:
            # process end of the first part in this handler loop 
            # because the server haven't recv the client's optimal info
            self.temp_filename_request_dict[(client_host, client_port)].append(filename)
            # can be multiple files requested because slave access info/determined info can be lost/jammed in Network

        # Get the list of slaves that have a copy of the file
        slave_string = self.get_slave_string(host, port)
        return_string = self.GET_RESPONSE % (host, port, filename, slave_string)
        return Distribute_pb2.dir_reply(result=return_string)
    
    def get_server_part2(self, request, context):
        _request = request.message.splitlines()
        client_host = _request[0].split(' ')[1]
        client_port = _request[1].split(' ')[1]
        optimal_slave_host = _request[2].split(' ')[1]
        optimal_slave_port = _request[3].split(' ')[1]
        # process all requested files from optimal slave to client
        for fi in self.temp_filename_request_dict[(client_host, client_port)]:
            slave_string = self.get_slave_string(optimal_slave_host, optimal_slave_port)
            return_string = self.GET_RESPONSE % (optimal_slave_host, optimal_slave_port, fi, slave_string)
            # con.sendall(return_string)
        self.temp_filename_request_dict[(client_host, client_port)] = []
        return Distribute_pb2.dir_reply(result=return_string)

    def send_slave_access_info2client(self, client_host, client_port):
        # send all slave info to the client
        tmp_str = ""
        for slave_host, slave_port in self.slave_nodes:
            access_info = self.slave_access_info[(slave_host, slave_port)]
            tmp_str += self.SEND_SLAVE_ACCESS_STATUS_HEADER % (slave_host, slave_port, access_info)
        return_str = self.SEND_SLAVE_ACCESS_STATUS_HEADER_TO_CLIENT % tmp_str
        self.my_send_request(return_str, client_host, client_port)
        # with grpc.insecure_channel("{0}:{1}".format(client_host, client_port)) as channel:
        #     dir_cil=Distribute_pb2_grpc.Direct_ServerStub(channel=channel)
        #     dir_cil.send_bare_info(Distribute_pb2.dir_request(message=return_str))
            # ?
        """
        ????????????
        """
        return 

    def get_slaves(self, request, context):
        # Function that operate the host to send the list of slave servers
        _request = request.message.splitlines()
        host = _request[0].split()[1]
        port = _request[1].split()[1]
        slave_string = self.get_slave_string(host, port)
        return_string = self.SLAVE_RESPONSE_HEADER % slave_string
        # print(return_string)
        #con.sendall(return_string)
        slaves = return_string.splitlines()[1:-1]
        return_list = []
        for i in range(0, len(slaves), 2):
            host = slaves[i].split()[1]
            port = slaves[i + 1].split()[1]
            return_list.append((host, port))
        self.num_slaves = len(return_list)
        self.slave_nodes = return_list
        return Distribute_pb2.dir_reply(result=return_string)

    def paxos_update_response_status(self, request, context):
        _request = request.message.splitlines()
        host = _request[0].split()[1]
        port = _request[1].split()[1]
        head, res = _request[2].split()
        head = head[:-1]
        if len(request) > 3:
            acceptor_timestamp = _request[3].split()[1]

        self.pok_sum[0] += int(head == 'ACCEPTOR_POK')
        self.pok_sum[1] += int(res == 'PoK')

        self.paxos_slave_acceptN_dict[(host, port)] = int(acceptor_timestamp)

        if self.paxos_cur_stage == 1 and self.pok_sum[0] == self.num_slaves:
            self.paxos_cur_stage = 2
            self.paxos_repeative_calling('check PoK')

        if self.paxos_cur_stage == 2:
            self.aok_sum[0] += int(head == 'ACCEPTOR_AOK')
            self.aok_sum[1] += int(res == 'AoK')

            if self.aok_sum[0] == self.num_slaves:
                self.paxos_repeative_calling('check AoK')
    #暂时不动
    def paxos_send_alldata_to_all_slaves(self, request, context):
        _request = request.message.splitlines()
        all_data = '\n\n'.join(_request[2:])
        send_string = self.SENDALL_DATA_TO_ALL_SLAVES_HEADER % all_data
        for (host, port) in self.slave_nodes:
            if self.chosen_slave == (host, port):
                continue
            self.my_send_request(send_string, host, port)
            # with grpc.insecure_channel("{0}:{1}".format(host, port)) as channel:
            #     dir_cil=Distribute_pb2_grpc.Direct_ServerStub(channel=channel)
            #     dir_cil.paxos_send_alldata_to_all_slaves(Distribute_pb2.dir_request(message=send_string))

    def paxos_proposer_send(self, purpose='send prepare'):
        """
        PAXOS
        Proposer, Acceptor, Learner
        consistency of whole database / all files
        master server: Proposer
        slave server: Acceptor
        no Learner
        """

        # Stage 1
        # Proposer: send prepare to all acceptors
        proposer_timestamp = time.time()
        if purpose == 'send prepare':
            send_string = self.PROPOSER_PREPARE_HEADER % proposer_timestamp
            for (host, port) in self.slave_nodes:
                self.my_send_request(send_string, host, port)
            return True
        elif purpose == 'check PoK':
            assert self.paxos_cur_stage == 2, 'Invalid PAXOS stage! End of current PAXOS Check'
            if self.pok_num[1] > self.num_slaves / 2:
                max_timestamp = 0
                for k, v in self.paxos_slave_acceptN_dict.items():
                    if v > max_timestamp:
                        max_timestamp = v
                        self.chosen_slave = k
                self.my_send_request(self.GETALL_DATA_FROM_A_SLAVE,  k[0], k[1])
                return True
                # wait for the acceptV and repeat to all slaves
            else:
                self.pok_sum = [0, 0]
                self.aok_sum = [0, 0]
                return False
                # repeatly sending prepare
        elif purpose == 'check AoK':
            assert self.paxos_cur_stage == 2, 'Invalid PAXOS stage! End of current PAXOS Check'
            if self.aok_num[1] > self.num_slaves / 2:
                self.paxos_cur_stage = 0
                print('PAXOS Consistency Check Passed!!!')
                return True
                # final confirmation! PAXOS end!
            else:
                self.pok_sum = [0, 0]
                self.aok_sum = [0, 0]
                return False
        else:
            print('purpose content: ', purpose, ' error! \
                Should be one of the \{send prepare, check PoK, check AoK\}')
            self.paxos_cur_stage = 0
            return False

    def paxos_repeative_calling(self, purpose = 'send prepare'):
        stage_res = False
        self.paxos_cur_stage = 1
        while not stage_res and self.paxos_trying_times < self.paxos_trying_times_limit:
            stage_res = self.paxos_proposer_send(purpose=purpose)
            if not stage_res:
                self.paxos_cur_stage = 1
                self.paxos_trying_times += 1
        if not stage_res:
            print("PAXOS Consistency Check Failed!!!")

    def paxos_proposer_send(self, purpose='send prepare'):
        """
        PAXOS
        Proposer, Acceptor, Learner
        consistency of whole database / all files
        master server: Proposer
        slave server: Acceptor
        no Learner
        """
        
        # Stage 1
        # Proposer: send prepare to all acceptors
        proposer_timestamp = time.time()
        if purpose == 'send prepare':
            send_string = self.PROPOSER_PREPARE_HEADER % proposer_timestamp
            for (host, port) in self.slave_nodes:
                self.my_send_request(send_string, host, port)

            return True
        elif purpose == 'check PoK':
            assert self.paxos_cur_stage == 2, 'Invalid PAXOS stage! End of current PAXOS Check'
            if self.pok_num[1] > self.num_slaves / 2:
                max_timestamp = 0
                for k, v in self.paxos_slave_acceptN_dict.items():
                    if v > max_timestamp:
                        max_timestamp = v
                        self.chosen_slave = k
                self.my_send_request(self.GETALL_DATA_FROM_A_SLAVE, k[0], k[1])

                return True
                # wait for the acceptV and repeat to all slaves
            else:
                self.pok_sum = [0, 0]
                self.aok_sum = [0, 0]
                return False
                # repeatly sending prepare
        elif purpose == 'check AoK':
            assert self.paxos_cur_stage == 2, 'Invalid PAXOS stage! End of current PAXOS Check'
            if self.aok_num[1] > self.num_slaves / 2:
                self.paxos_cur_stage = 0
                print('PAXOS Consistency Check Passed!!!')
                return True
                # final confirmation! PAXOS end!
            else:
                self.pok_sum = [0, 0]
                self.aok_sum = [0, 0]
                return False
        else:
            print('purpose content: ', purpose, ' error! \
                Should be one of the \{send prepare, check PoK, check AoK\}')
            self.paxos_cur_stage = 0
            return False

    def paxos_send_alldata_to_all_slaves(self, con, addr, text):

        request = text.splitlines()
        all_data = '\n\n'.join(request[2:])
        send_string = self.SENDALL_DATA_TO_ALL_SLAVES_HEADER % all_data
        for (host, port) in self.slave_nodes:
            if self.chosen_slave == (host, port):
                continue
            self.my_send_request(send_string, host, port)

    def paxos_repeative_calling(self, purpose = 'send prepare'):
        stage_res = False
        self.paxos_cur_stage = 1
        while not stage_res and self.paxos_trying_times < self.paxos_trying_times_limit:
            stage_res = self.paxos_proposer_send(purpose=purpose)
            if not stage_res:
                self.paxos_cur_stage = 1
                self.paxos_trying_times += 1
        if not stage_res:
            print("PAXOS Consistency Check Failed!!!")

    def paxos_update_response_status(self, con, addr, text):
        request = text.splitlines()
        host = request[0].split()[1]
        port = request[1].split()[1]
        head, res = request[2].split()
        head = head[:-1]
        if len(request) > 3:
            acceptor_timestamp = request[3].split()[1]

        self.pok_sum[0] += int(head == 'ACCEPTOR_POK')
        self.pok_sum[1] += int(res == 'PoK')

        self.paxos_slave_acceptN_dict[(host, port)] = int(acceptor_timestamp) 

        if self.paxos_cur_stage == 1 and self.pok_sum[0] == self.num_slaves:
            self.paxos_cur_stage = 2
            self.paxos_repeative_calling('check PoK')

        if self.paxos_cur_stage == 2:            
            self.aok_sum[0] += int(head == 'ACCEPTOR_AOK')
            self.aok_sum[1] += int(res == 'AoK')

            if self.aok_sum[0] == self.num_slaves:
                self.paxos_repeative_calling('check AoK')


    #暂时不动
    def slave_fileserver_access_update(self, request, context):
        _request = request.message.splitlines()
        host = _request[0].split()[1]
        port = _request[1].split()[1]
        access_freq = int(_request[2].split()[1])
        self.slave_access_info[(host, port)] = access_freq
        
    #底层函数定义
    # 1st: num hoops
    def slave_fileserver_distribute(self, all_slave_hosts, client_host, client_port, strategy = 'random'):
        if strategy == 'random':
            chosen_host, chosen_port = random.choice(all_slave_hosts)[0]
        elif strategy == 'Load Balancing and Traffic Management':
            chosen_host, chosen_port = self.send_slave_access_info2client(client_host, client_port)

        return chosen_host, chosen_port#？

    def slave_fileserver_distribute_prepare(self, all_slave_hosts, client_host, client_port):
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
            min_time = arr[0].split('=')[1][1:-2]
            max_time = arr[1].split('=')[1][1:-2]
            avg_time = arr[2].split('=')[1][1:-2]

            if (host, port) not in self.slave_access_info:
                self.my_send_request(self.GET_SLAVE_ACCESS_STATUS_HEADER, host, port)
                access_info = None
            else:
                access_info = self.slave_access_info[(host, port)]
                # remain -= 1

            res_arr.append((num_hoops, [min_time, max_time, avg_time], access_info))
        self.client2slaves_access_info[(client_host, client_port)] = res_arr


    def find_host(self, path):
        # Function that takes a path and returns the server that contains that directories files
        return_host = (False, False)
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Server FROM Directories WHERE Path = ?", (path,))
            server = cur.fetchone()
            if server:
                server_id = server[0]
                cur = con.cursor()
                cur.execute("SELECT Server, Port FROM Servers WHERE Id = ?", (server_id,))
                return_host_list = cur.fetchall()
        return return_host_list

    def pick_random_host(self):
        # Function to pick a random host from the database
        return_host = False
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Id FROM Servers")
            servers = cur.fetchall()
            if servers:
                return_host = random.choice(servers)[0]
        return return_host

    def get_slave_string(self, host, port):
        # Function that generates a slave string
        return_string = ""
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Server, Port FROM Servers WHERE NOT (Server=? AND Port=?)", (host, port,))
            servers = cur.fetchall()
        for (host, port) in servers:
            # slaves' info: host, port
            header = self.SLAVE_HEADER % (host, port)
            return_string = return_string + header
        return return_string

    def pick_random_host(self):
        # Function to pick a random host from the database
        return_host = False
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Id FROM Servers")
            servers = cur.fetchall()
            if servers:
                return_host = random.choice(servers)[0]
        return return_host

    def create_dir(self, path, host):
        # Function to create a directory in the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Directories (Path, Server) VALUES (?, ?)", (path, host,))
        con.commit()
        con.close()

    def add_server(self, host, port):
        # Function to add a server to the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Servers (Server, Port) VALUES (?, ?)", (host, port,))
        con.commit()
        con.close()

    def remove_dir(self, path):
        # Function to remove a directory from the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM Directories WHERE Path = ?", (path,))
        con.commit()
        con.close()

    def remove_server(self, server):
        # Function to remove a server from the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM Servers WHERE Server = ?", (server,))
        con.commit()
        con.close()

    def create_tables(self):
        # Function to add the tables to the database
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Servers(Id INTEGER PRIMARY KEY, Server TEXT, Port TEXT)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS SERVS ON Servers(Server, Port)")
            cur.execute("CREATE TABLE IF NOT EXISTS Directories(Id INTEGER PRIMARY KEY, Path TEXT, Server INTEGER, FOREIGN KEY(Server) REFERENCES Servers(Id))")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS DIRS ON Directories(Path)")

def main():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 实例化 计算len的类
    servicer = Direct_Server()
    # 注册本地服务,方法ComputeServicer只有这个是变的
    Distribute_pb2_grpc.add_Direct_ServerServicer_to_server(servicer, server)
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
