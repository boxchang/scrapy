#!/usr/bin/python
import sys
import time

sys.path.append("..")
import sqlite3
from sqlite3 import Error

from jobs.line import callBoxLine
from jobs.models import JobModel
from web104.database import database


def create_connection(db_file):
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


def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def select_task_by_priority(conn, priority):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE priority=?", (priority,))

    rows = cur.fetchall()

    for row in rows:
        print(row)


def update_job_read(row):
    pass


def main():
    # database = "D:\\0)SourceCode\\scrapy\\web104\\web104.sqlite"

    # create a database connection
    db = database()
    conn = db.create_connection()

    cur = conn.cursor()
    cur.execute("SELECT * FROM web104 where is_read is null or is_read='' ")

    rows = cur.fetchall()

    for row in rows:
        job = JobModel(row)
        if job.validate():
            message = job.custName + ' --- '+ job.jobName + '\n\r' + job.addr + '\n\r' + job.jobLink
            callBoxLine(message)
            time.sleep(1)
    try:
        cur.execute("UPDATE web104 SET is_read = 'Y' where is_read is null or is_read=''")
        conn.commit()
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()
