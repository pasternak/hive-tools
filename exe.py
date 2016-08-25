#!/usr/bin/env python3
import MySQLdb
import argparse
import asyncio
import re
import sys
import itertools
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

class MySQLConnection(object):

    def __init__(self):
        self.host = '127.0.0.1'
        self.user = 'root'
        self.port = 3306
        self.passwd = 'Password'
        self.db = 'hive'
        self.cursor = None
        self.spinner = itertools.cycle(['-', '/', '|', '\\'])

    def connect(self):
        db = MySQLdb.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.db)
        db.autocommit(True)
        return db.cursor()

    def execute(self, query, args=None):
        sys.stdout.write("Updating...{}".format(next(self.spinner)))
        sys.stdout.flush()
        sys.stdout.write('\r')
        if not self.cursor:
            self.cursor = self.connect()
        if args:
            self.cursor.executemany(query, args)
            return None
        else:
            self.cursor.execute(query)
            return self.cursor.fetchall()


class HiveMetastore(object):
    EXECUTORS = 50

    @staticmethod
    def listFSRoot():
        meta = MySQLConnection()
        for location in meta.execute("select DB_LOCATION_URI from DBS"):
            print(location[0])

    @staticmethod
    def updateFS(update):
        meta = MySQLConnection()

        executor = ThreadPoolExecutor(max_workers=1)
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue()
        locations = meta.execute(
            "select LOCATION from SDS where LOCATION like '{}/%'".format(update['old']))
        for location in locations:
            queue.put_nowait(location[0])

        values = loop.run_until_complete(
            HiveMetastore.build_tuple(update, queue))
        while values:
            for _ in range(HiveMetastore.EXECUTORS):
                if threading.active_count() <= HiveMetastore.EXECUTORS:
                    try:
                        loop.run_in_executor(executor, meta.execute, "update SDS set LOCATION='{0}' where LOCATION='{1}'".format(*values.pop()))
                    except: pass
        # meta.execute("update SDS set LOCATION=%s where LOCATION=%s", values)

    @staticmethod
    async def build_tuple(update, queue):
        values = []
        while not queue.empty():
            item = await queue.get()
            n_item = re.sub(update['old'], update['new'], item)

            values.append(
                (n_item, item)
            )
        return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--listFSRoot', action='store_true',
                        help='List hive FSRoots')
    parser.add_argument('--updateFS', default=[], nargs=2,
                        help='Update NameNode endpoint in hive metastore')
    # parser.add_argument('-c', '--config', default=None, required=True,
    #                    help='Path to config file')
    args = parser.parse_args()

    # List all HDFS locations
    if args.listFSRoot:
        HiveMetastore.listFSRoot()
    # Build a list of concurrent tasks to be executed by asyncio
    if args.updateFS:
        HiveMetastore.updateFS(
            {'old': args.updateFS[0], 'new': args.updateFS[1]})
    # Commit all changes to the DB
    # meta.commit()

if __name__ == "__main__":
    main()
