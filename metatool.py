#!/usr/bin/env python3

import os
import re
import sys
import time
import asyncio
import argparse
import configparser
import pymysql.cursors


class MysqlConnection(object):
    """ Manage MySQL metastore connections/operations """

    def __init__(self, path):
        """ Create MysqlConnection object and init path to config file
        :param path: the path to config file
        :type  path: String
        """
        self.path = path
        self.config = None
        self.connection = None

    def __config(self):
        """ Read config file and return mysql section """
        if not os.path.exists(self.path):
            raise IOError(self.path)

        config = configparser.ConfigParser()
        config.read(self.path)
        if 'mysql' in config:
            return config['mysql']
        else:
            print("Error: there is no [mysql] section in config file!")
            sys.exit(1)

    def connect(self):
        """ Return MySQL connection descriptor """
        config = self.__config()
        return pymysql.connect(**dict(config.items()),
                               cursorclass=pymysql.cursors.DictCursor,
                               autocommit=False)

    def commit(self):
        """ Commit changes to the database """
        if self.connection:
            connection.comit()

    async def execute(self, query, queue=None, val=None):
        """ Run MySQL queries in asynchronous mode
        :param query: The query to be triggered
        :type  query: String
        :param queue: The queue to pull queries from
        :type  queue: asyncio.Queue
        :param val: New value for SET and Insert
        :type  val: String
        """
        if not self.connection:
            self.connection = self.connect()

        with self.connection.cursor() as cursor:
            while not queue.empty():
                item = await queue.get()
                if val:
                    n_item = re.sub(val[0], val[1], item)
                    print("\rExecuting statemnt on {0}".format(item), end="", flush=True)
                if "{}/".format(val[1]) not in item:
                    pass
                    # print(query.format(n_item, item))
                await asyncio.sleep(0)


class HiveMetastore(object):
    EXECUTORS = 60

    @classmethod
    def listFSRoot(cls, config):
        # Create object to operate on hive metastore objects
        meta = MysqlConnection(config)
        conn = meta.connect()
        with conn.cursor() as cursor:
            cursor.execute("SELECT DB_LOCATION_URI from DBS")
            for row in cursor.fetchall():
                print(row['DB_LOCATION_URI'])

    def _tables(cls, meta, loop):
        # Make update operation on SDS table
        conn = meta.connect()

        sync_q = asyncio.Queue(loop=loop)
        # Update hive files endpoints
        with conn.cursor() as cursor:
            # Populate asyncio Queue with the data from query
            cursor.execute("SELECT LOCATION from SDS where LOCATION like '{0}/%'".format(cls.src))
            for row in cursor.fetchall():
                sync_q.put_nowait(row['LOCATION'])
            print("[-] Number of elements in queue: {0}".format(sync_q.qsize()))
            tasks = [
                asyncio.ensure_future(
                    meta.execute("UPDATE SDS SET LOCATION='{0}' where LOCATION='{1}'", sync_q, [cls.src, cls.dst]))
                for num in range(cls.EXECUTORS)
            ]

            loop.run_until_complete(asyncio.wait(tasks))
            print(" DONE")

    def _databases(cls, meta, loop):
        # Make update operation on DBS databases
        conn = meta.connect()

        sync_d = asyncio.Queue(loop=loop)
        # Update hive files endpoints
        with conn.cursor() as cursor:
            # Populate asyncio Queue with the data from query
            cursor.execute("SELECT DB_LOCATION_URI from DBS where DB_LOCATION_URI like '{0}/%'".format(cls.src))
            for row in cursor.fetchall():
                sync_d.put_nowait(row['DB_LOCATION_URI'])
            print("[-] Number of elements in queue: {0}".format(sync_d.qsize()))
            tasks = [
                asyncio.ensure_future(
                    meta.execute("UPDATE DBS SET DB_LOCATION_URI='{0}' where DB_LOCATION_URI='{1}'", sync_d, [cls.src, cls.dst]))
            ]

            loop.run_until_complete(asyncio.wait(tasks))
            print(" DONE")

    @classmethod
    def updateFS(cls, src, dst, config):
        """ Update NameNode endpoint
        :param src: Old hdfs/namenode address we want to replace
        :type  src: String
        :param dst: New hdfs/namenode address we want to set
        :type  dst: String
        """
        cls.src = src
        cls.dst = dst
        # Build Queue to keep gathered selects
        loop = asyncio.get_event_loop()

        # Create object to operate on hive metastore objects
        meta = MysqlConnection("hive.cfg")
        for update in ['tables', 'databases']:
            getattr(cls, "_{0}".format(update))(cls, meta, loop)

        loop.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--listFSRoot', action='store_true',
                        help='List hive FSRoots')
    parser.add_argument('--updateFS', default=[], nargs=2,
                        help='Update NameNode endpoint in hive metastore')
    parser.add_argument('-c', '--config', default=None, required=True,
                        help='Path to config file')
    args = parser.parse_args()

    # List all HDFS locations
    if args.listFSRoot:
        HiveMetastore.listFSRoot(args.config)
    # Build a list of concurrent tasks to be executed by asyncio
    if args.updateFS:
        HiveMetastore.updateFS(args.updateFS[0], args.updateFS[1], args.config)
    # Commit all changes to the DB
    # meta.commit()

if __name__ == "__main__":
    main()
