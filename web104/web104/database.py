import sqlite3
from sqlite3 import Error
import MySQLdb

#from web104.config import DATABASE_PORT, DATABASE_PASSWD, DATABASE_HOST, DATABASE_USER, DATABASE_DB


class database:

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
                                   db="web104",
                                   port=3307, use_unicode=True, charset="utf8")  # name of the data base

            return conn
        except Error as e:
            print(e)

        return None
