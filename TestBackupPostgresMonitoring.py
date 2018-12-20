#! /usr/bin/python
# *-* coding: utf-8 *-*
"""
Test backup restore  script
"""

import logging
import socket
import sys
import os
import argparse
import ConfigParser
import psycopg2
from configparser import SafeConfigParser
from prometheus_client import Gauge, CollectorRegistry, pushadd_to_gateway

class Config(object):
    """ Config class get a configuration file return a dictionnary
        of section and params """
    def __init__(self, config_file):
        self.filename = config_file
        self.configuration = {}
        self.read_config_file()

    def read_config_file(self):
        """ Read config file and parse database """
        parser = SafeConfigParser()
        try:
            parser.read(self.filename)
        except IOError as error:
            logging.info("Cannot read file: {}.".format(error))
            sys.exit(0)

        options_name = [
            'host',
            'user',
            'database',
            'password'
            ]

        for section_name in parser:
            self.configuration[section_name] = {}
            if section_name != 'DEFAULT':
                for option in options_name:
                    try:
                        value = parser.get(section_name, option)
                        self.configuration[section_name][option] = value
                    except ConfigParser.NoOptionError, err:
                        logging.info(str(err))
                        sys.exit(0)

class Postgres(object):

    """
    This Class can connect on Postgresql database execute a query
    and close the connection to the database
    """

    def __init__(self, params_postgresql):
        self.psqldriver = self.connect(params_postgresql)

    def connect(self, params_postgresql):
        """
        Connect to the PostgreSQL database server
        """
        try:
            logging.info('Connecting to the PostgreSQL database...\n')
            return psycopg2.connect(**params_postgresql)
        except psycopg2.DatabaseError as error:
            logging.info("Connection error: {}.".format(error))
            sys.exit(0)

    def query(self, query):
        """
        Take a SQL query return a dictionoray of SQL result
        """
        cursor = self.psqldriver.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.Error as error:
            logging.info("ERROR: Error in execute query: {}.".format(error))
            sys.exit(0)

    def close(self):
        """
        Close Postgresql connection
        """
        if self.psqldriver is not None:
            self.psqldriver.close()
            logging.info('Database connection closed.')
            sys.exit(0)

def get_db_size(obj_db):
    """
    query get the size of database
    """
    query = (""" SELECT pg_database.datname
                 AS database_name,
                 pg_database_size(pg_database.datname)
                 AS database_size_bytes
                 FROM pg_database ;""")

    return obj_db.query(query)

def monitoring(**kwargs):
    """
    Translate database size in metrics for Prometheus
    """
    try:
        registry = CollectorRegistry()
        gauge = Gauge('pg_database_size_restore',
                      'database size of backup restore',
                      ["hostname"], registry=registry)

        gauge.labels(kwargs['hostname']).set(kwargs['size'])

        pushadd_to_gateway(kwargs['pushgateway_url'],
                           job=kwargs['job_name'],
                           grouping_key=kwargs['instance'],
                           registry=registry)
    except Exception as error:
        logging.info("ERROR: Cannot push to pushgateway: {}.".format(error))
        sys.exit(0)

def gethostname():
    """
    Return Hostname
    """
    try:
        return socket.gethostname()
    except socket.error as error:
        logging.info("ERROR: Cannot return hostname of the server: {}.".format(error))
        sys.exit(0)

def getipaddress():
    """
    Return ip address based on /etc/host in dict format for Prometheus
    """
    try:
        return {'instance':socket.gethostbyname(socket.gethostname())}
    except socket.error as error:
        logging.info("ERROR: Cannot return the ip adress of the server: {}.".format(error))
        sys.exit(0)
def main():
    """
    Main module
    """
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(description="Monitoring arguments.")

    # Arguments
    parser.add_argument("-u", "--url",
                        metavar="pushgateway url",
                        dest="url", help="http://10.150.24.6:9091",
                        required=True)

    parser.add_argument("-c", "--config-file",
                        metavar="configfile",
                        dest="config_file",
                        help="databases.ini", required=True)
    args = parser.parse_args()

    # Need a config file to connect to Postgresql database

    if os.stat(args.config_file).st_size != 0:
        config = Config(args.config_file)
        for section_name in config.configuration:
            if section_name != 'DEFAULT':
                database = Postgres(config.configuration[section_name])
                for database_name, size_in_bytes in get_db_size(database):
                    monitoring(pushgateway_url=args.url,
                               job_name=database_name,
                               hostname=gethostname(),
                               instance=getipaddress(),
                               size=size_in_bytes)

                database.close()
    else:
        logging.info("ERROR: file is empty")
        sys.exit(0)


if __name__ == '__main__':
    main()
