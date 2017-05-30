#!/usr/bin/env python2

######################################################
#
# Consumer: Read kafka messages. Allows user to
# choose among different output sources for writing messages:
# console, file or database
# written by Abrar Hussain (abrarhussainturi@gmail.com)
#
######################################################


import argparse
import logging
import os
import mysql

from kafka import KafkaProducer, KafkaConsumer
from mysql.connector import errorcode

mysql_configs = {
    'user': 'test',
    'password': 'test',
    'host': '127.0.0.1',
    'database': 'kafka',
    'raise_on_warnings': True,
}

TOPIC_NAME = 'customers'
KAFKA_HOST = 'kafka'
KAFKA_PORT = '9092';
WRITE_TO_FILE_PATH = os.path.abspath(os.path.join(__file__,'../../../resources/consumed_msgs'))

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Consume messages from Kafka",
        epilog="Example Usage: "
               "python consumer.py -c, "
               "python consumer.py -f [file-path], "
               "python consumer.py -d")

    parser.add_argument("-c", "--screen_output",
                        dest="screen_output",
                        action="store_true",
                        help="Write to console")

    parser.add_argument("-f", "--file",
                        dest="file",
                        action="store_true",
                        help="Write to file")

    parser.add_argument("-d", "--database",
                        dest="database",
                        action="store_true",
                        help="Write to database")

    parser.add_argument("-v", "--verbose",
                        dest="verbose",
                        action="store_true",
                        help="enable debug logging")

    return parser.parse_args()


def consume_messages(dest):
    args = parse_arguments()
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_HOST + ':' + KAFKA_PORT])
    for message in consumer:
        # write to screen
        if dest is args.screen_output:
            print(message.value)

        # write to file
        if dest is args.file:
            write_to_file(message.value)

        # write to db
        if dest is args.database:
            # write to database
            add_new_customer(message.value)


def write_to_file(v):
    if not v:
        return
    with open(WRITE_TO_FILE_PATH, "a+") as fp:
        fp.write("\n" + v)


def add_new_customer(v):
    if not v:
        return

    conn = connect_to_mysql()
    cursor = conn.cursor()
    sql = "INSERT INTO customers (customer_name) VALUES (%s)"
    cursor.execute(sql, (v,))
    conn.commit()
    conn.close


def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**mysql_configs)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")

        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
            return;
    else:
        return conn;


def main():
    args = parse_arguments()
    level = logging.INFO

    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    # User input
    if args.screen_output:
        # get message from kafka
        consume_messages(args.screen_output)

    if args.file:
        consume_messages(args.file)

    if args.database:
        consume_messages(args.database)

if __name__ == "__main__":
    main()
