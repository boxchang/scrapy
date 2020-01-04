import sqlite3
from sqlite3 import Error
import MySQLdb

class database:

    def execute_sql(self, sql):
        self.conn = self.create_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()


    def create_sqlite_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(db_file)

            return conn
        except Error as e:
            print(e)

        return None


    def create_connection(self):
        try:
            conn = MySQLdb.connect(host="111.185.227.34",  # your host, usually localhost
                                   user="web104",  # your username
                                   passwd="cnap*74182",  # your password
                                   db="stock",
                                   port=3307, use_unicode=True, charset="utf8")  # name of the data base

            return conn
        except Error as e:
            print(e)

        return None
