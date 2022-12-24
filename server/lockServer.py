
import socket
import re
import sys
import time
import sqlite3 as db

from collections import defaultdict
from tcpServer import TCPServer

lock_mapping_dict = {
    'EXCLUSIVE': 3,
    'RESERVED': 2,
    'SHARED': 1,
    'PENDING': 0
}

class LockServer(TCPServer):
    LOCK_REGEX = "LOCK_FILE: [a-zA-Z0-9_./]*\nTime: [0-9]*\n\n"
    UNLOCK_REGEX = "UNLOCK_FILE: [a-zA-Z0-9_./]*\n\n"
    LOCK_RESPONSE = "LOCK_RESPONSE: \nLOCK_TYPE: \nFILENAME: %s\nTIME: %d\n\n"
    FAIL_RESPONSE = "ERROR: %d\nMESSAGE: %s\n\n"
    # DATABASE = 'Database/locking.db'

    def __init__(self, port_use=None):
        TCPServer.__init__(self, port_use, self.handler)
        self.create_table()
        self.lock_status_dict = defaultdict(list)

    #匹配正则表达式，确定操作上锁or解锁并调用函数
    def handler(self, message, con, addr):
        if re.match(self.LOCK_REGEX, message):
            self.get_lock(con, addr, message)
        elif re.match(self.UNLOCK_REGEX, message):
            self.get_unlock(con, addr, message)
        else:
            return False
        return True

    def get_lock(self, con, addr, text):
        # Handler for file locking requests
        request = text.splitlines()
        full_path = request[0].split()[1]#获取目标路径
        lock_type = request[1].split()[1]#锁类型：互斥/兼容/...
        duration = int(request[2].split()[1])#获取时间

        lock_time = self.lock_file(full_path, lock_type, duration)
        if lock_time:
            return_string = self.LOCK_RESPONSE % (full_path, lock_type, lock_time)
        else:
            return_string = self.FAIL_RESPONSE % (0, str(duration))
        con.sendall(return_string)
        return

    def get_unlock(self, con, addr, text):
        # Handler for file unlocking requests
        request = text.splitlines()
        full_path = request[0].split()[1]
        lock_type = request[1].split()[1]#锁类型：互斥/兼容/...
        lock_time = self.unlock_file(full_path, lock_type)

        return_string = self.LOCK_RESPONSE % (full_path, lock_type, lock_time)
        con.sendall(return_string)
        return

    def lock_file(self, path, lock_type, lock_period):
        # Function that attempts to lock a file
        return_time = -1
        # lock_type: read/write
        con = db.connect(self.DATABASE)

        if lock_type == 'read':
            con.isolation_level = 'SHARED'
            con.execute('BEGIN PENDING')
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
        '''
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
        '''
        # End r/w access to the db
        con.commit()
        con.close()
        return return_time

    def unlock_file(self, lock_type, path):
        # Function that attempts to unlock a file
        return_time = -1
        con = db.connect(self.DATABASE)
        # Exclusive r/w access to the db
        con.isolation_level = lock_type
        con.execute('BEGIN {}'.format(lock_type))
        current_time = int(time.time())
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time))
        count = cur.fetchone()[0]
        if count is 0:
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
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port = int(sys.argv[1])
            server = LockServer(port)
        else:
            server = LockServer()
        server.listen()
    except socket.error as msg:
        print("Unable to create socket connection: " + str(msg))
        con = None


if __name__ == "__main__": main()
