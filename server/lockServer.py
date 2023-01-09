# -*-coding:utf-8-*-
# 编辑者：XuZH
import grpc
import Distribute_pb2
import Distribute_pb2_grpc
from concurrent import futures
import socket
import re
import sys
import time
import sqlite3 as db
from collections import defaultdict


# 由于grpc包含Tcp的实现，所以无需再借助tcpserver
class Lock_Server(Distribute_pb2_grpc.Lock_ServerServicer):
    LOCK_RESPONSE = "LOCK_RESPONSE: \nLOCK_TYPE: \nFILENAME: %s\nTIME: %d\n\n"
    FAIL_RESPONSE = "ERROR: %d\nMESSAGE: %s\n\n"
    DATABASE = "Database/lock.db"
    # 差个__init__

    def get_lock(self, request, context):
        _request = request.message.splitlines()
        full_path = _request[0].split()[1]  # 获取目标路径
        lock_type = _request[1].split()[1]  # 锁类型：互斥/兼容/...
        duration = int(_request[2].split()[1])  # 获取时间

        lock_time = self.lock_file(full_path, lock_type, duration)
        if lock_time:
            return_string = self.LOCK_RESPONSE % (full_path, lock_type, lock_time)
        else:
            return_string = self.FAIL_RESPONSE % (0, str(duration))
        return Distribute_pb2.lock_reply(result=return_string)

    def get_unlock(self, request, context):
        _request = request.message.splitlines()
        full_path = _request[0].split()[1]  # 获取目标路径
        lock_type = _request[1].split()[1]  # 锁类型：互斥/兼容/...
        lock_time = self.unlock_file(full_path, lock_type)
        return_string = self.LOCK_RESPONSE % (full_path, lock_type, lock_time)
        return Distribute_pb2.lock_reply(result=return_string)

    def lock_file(self, path, lock_type, lock_period):
        # Function that attempts to lock a file
        # lock_type: read/write
        con = db.connect(self.DATABASE)
        if lock_type == 'read':
            con.isolation_level = 'SHARED'
            # con.execute('BEGIN PENDING')
            con.execute('BEGIN SHARED')

        elif lock_type == 'write':
            con.isolation_level = 'EXCLUSIVE'
            con.execute('BEGIN EXCLUSIVE')
        else:
            con.close()
            return False
            # print('lock_type Error!')
        current_time = int(time.time())
        end_time = current_time + lock_period
        # '''
        cur = con.cursor()
        # 找锁
        cur.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time))
        count = cur.fetchone()[0]
        if count == 0:
            # 新锁
            cur.execute("INSERT INTO Locks (Path, Time) VALUES (?, ?)", (path, end_time))
            return_time = end_time
        else:
            return_time = False
        # '''
        # End r/w access to the db
        con.commit()
        con.close()
        return return_time

    def unlock_file(self, lock_type, path):
        # Function that attempts to unlock a file
        con = db.connect(self.DATABASE)
        # Exclusive r/w access to the db
        if lock_type == 'read':
            con.isolation_level = 'SHARED'
            # con.execute('BEGIN PENDING')
            con.execute('BEGIN SHARED')

        elif lock_type == 'write':
            con.isolation_level = 'EXCLUSIVE'
            con.execute('BEGIN EXCLUSIVE')
        else:
            con.close()
            return False
        current_time = int(time.time())
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time))
        count = cur.fetchone()[0]
        #曾经是is
        if count == 0:
            cur.execute("UPDATE Locks SET Time=? WHERE Path = ? AND Time > ?", (current_time, path, current_time))
        # End r/w access to the db
        con.commit()
        con.close()
        return current_time

    '''
    def create_table(cls):
        # Function that creates the tables for the locking servers database
        con = db.connect(cls.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Locks(Id INTEGER PRIMARY KEY, Path TEXT, Time INT)")
            cur.execute("CREATE INDEX IF NOT EXISTS PATHS ON Locks(Path)")
    '''
    
def main():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 实例化 计算len的类
    servicer = Lock_Server()
    # 注册本地服务,方法ComputeServicer只有这个是变的
    Distribute_pb2_grpc.add_Lock_ServerServicer_to_server(servicer, server)
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
