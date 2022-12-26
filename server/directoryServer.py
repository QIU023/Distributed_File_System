
import socket
import re
import sys
import os
import hashlib
import random
import sqlite3 as db
import time

from tcpServer import TCPServer

# host server
class DirectoryServer(TCPServer):
    GET_REGEX = "GET_SERVER: \nCLIENT_HOST: [a-zA-Z0-9_./]*\nCLIENT_PORT: [0-9_./]*\n\FILENAME: [a-zA-Z0-9_./]*\n\n"
    GET_RESPONSE = "PRIMARY_SERVER: %s\nPORT: %s\nFILENAME: %s%s\n\n"
    GET_SLAVES_REGEX = "GET_SLAVES: .*\nPORT: [0-9]*\n\n"
    SLAVE_RESPONSE_HEADER = "SLAVES: %s\n\n"
    SLAVE_HEADER = "\nSLAVE_SERVER: %s\nPORT: %s"
    CREATE_DIR_REGEX = "CREATE_DIR: \nDIRECTORY: [a-zA-Z0-9_./]*\n\n"
    DELETE_DIR_REGEX = "DELETE_DIR: \nDIRECTORY: [a-zA-Z0-9_./]*\n\n"
    GETALL_DATA_FROM_A_SLAVE = "SENDALL_DATA_TO_MASTER\n\n"
    SENDALL_DATA_TO_ALL_SLAVES_HEADER = "SENDALL_DATA_TO_ALL_SLAVES_HEADER\n\n%s"

    DATABASE = "Database/directories.db"

    DIR_HOST = "0.0.0.0"
    DIR_PORT = 8005         # master/proposer's port
    CHECK_INTERVAL = 60

    # paxos consistency message headers, for master / proposer
    PAXOS_CHECK_REGEX = "PAXOS_CHECK\n\n"
    PROPOSER_PREPARE_HEADER = "PROPOSER_PREPARE_N: %s\n\n"    
    PROPOSER_ACCEPT_HEADER = "PROPOSER_ACCEPT_N: %s\nPROPOSER_ACCEPT_V: %s\n\n"
    ACCEPTOR_POK_REGEX = "HOST: [a-zA-Z0-9_.]*\n\PORT: [0-9_.]*\nACCEPTOR_POK: [a-zA-Z0-9_.]*\nACCEPTOR_ACCEPT_N: [0-9_.]*\n\nACCEPTOR_ACCEPT_V: .*\n\n"
    ACCEPTOR_AOK_REGEX = "HOST: [a-zA-Z0-9_.]*\nPORT: [0-9_.]*\nACCEPTOR_AOK: [a-zA-Z0-9_.]*\n"
    
    SENDALL_DATA_REGEX = "ALL_DATA_TO_MASTER\nACCEPT_N: [0-9_.]\nALLDATA: [a-zA-Z0-9_.]*\n\n"
    

    def __init__(self, port_use=None):
        TCPServer.__init__(self, port_use, self.handler)
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


    def handler(self, message, con, addr):
        
        if (time.time() - self.paxos_previous_check_time) % self.paxos_check_interval == 0:
            self.paxos_cur_stage = 1
            self.paxos_repeative_calling()

        if re.match(self.GET_REGEX, message):
            self.get_server(con, addr, message)
        elif re.match(self.GET_SLAVES_REGEX, message):
            self.get_slaves(con, addr, message)
        elif re.match(self.ACCEPTOR_POK_REGEX, message) or re.match(self.ACCEPTOR_AOK_REGEX, message):
            # confirming PoK/AoK, operate the paxos status
            self.paxos_update_response_status(con, addr, message)
        elif re.match(self.SENDALL_DATA_REGEX, message):
            # already confirmed sum(AoK) > n / 2, sychonize data to all slaves
            self.paxos_send_alldata_to_all_slaves(con, addr, message)

        else:
            return False



        return True

    def get_server(self, con, addr, text):
        # Handler for file upload requests
        request = text.splitlines()
        client_host = request[1]
        client_port = int(request[2])
        full_path = request[3].split()[1]

        path, file = os.path.split(full_path)
        name, ext = os.path.splitext(file)
        filename = hashlib.sha256(full_path).hexdigest() + ext
        all_slave_hosts = self.find_host(path)
        host, port = self.slave_fileserver_distribute(all_slave_hosts, client_host, client_port)

        if not host:
            # The Directory doesn't exist and must be added to the db
            server_id = self.pick_random_host()
            self.create_dir(path, server_id)
            all_slave_hosts = self.find_host(path)
            host, port = self.slave_fileserver_distribute(all_slave_hosts, client_host, client_port)

        # Get the list of slaves that have a copy of the file
        slave_string = self.get_slave_string(host, port)
        return_string = self.GET_RESPONSE % (host, port, filename, slave_string)
        # print(return_string)
        con.sendall(return_string)
        return

    
    def slave_fileserver_distribute(self, all_slave_hosts, client_host, client_port):
        # 流量管理算法/负载均衡算法
        # 获得所有slaves与client的ping延时？ 

        return chosen_host, chosen_port


    def get_slaves(self, con, addr, text):
        # Function that operate the host to send the list of slave servers
        request = text.splitlines()
        host = request[0].split()[1]
        port = request[1].split()[1]
        slave_string = self.get_slave_string(host, port)
        return_string = self.SLAVE_RESPONSE_HEADER % slave_string
        # print(return_string)
        con.sendall(return_string)
        slaves = return_string.splitlines()[1:-1]
        return_list = []
        for i in range(0, len(slaves), 2):
            host = slaves[i].split()[1]
            port = slaves[i + 1].split()[1]
            return_list.append((host, port))
        self.num_slaves = len(return_list)
        self.slave_nodes = return_list

        return

    # def paxos_consistency_check(self, con, addr, text):
        
    #     pass


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
                self.send_request(send_string, host, int(port))
            return True
        elif purpose == 'check PoK':
            assert self.paxos_cur_stage == 2, 'Invalid PAXOS stage! End of current PAXOS Check'
            if self.pok_num[1] > self.num_slaves / 2:
                max_timestamp = 0
                for k, v in self.paxos_slave_acceptN_dict.items():
                    if v > max_timestamp:
                        max_timestamp = v
                        self.chosen_slave = k
                self.send_request(self.GETALL_DATA_FROM_A_SLAVE, k[0], int(k[1]))
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
            self.send_request(send_string, host, int(port))

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
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port = int(sys.argv[1])
            server = DirectoryServer(port)
        else:
            server = DirectoryServer()
        server.listen()
    except socket.error as msg:
        print("Unable to create socket connection: " + str(msg))
        con = None


if __name__ == "__main__": main()
