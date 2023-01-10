import sqlite3 as db
import sys

DATABASE = "Database/directories.db"
con = db.connect(DATABASE)
with con:
    cur = con.cursor()
    cur.execute('INSERT INTO Servers (Server, Port) VALUES ("127.0.0.1", 8010)')
    cur.execute('INSERT INTO Servers (Server, Port) VALUES ("127.0.0.1", 8011)')
    cur.execute('INSERT INTO Servers (Server, Port) VALUES ("127.0.0.1", 8012)')
    con.commit()
    cur.close()