#!/usr/bin/env python2

######################################################
#
# producer: Write message to Kafka topic. Allows user to
# choose among different input sources: console, file or
# database
# written by Abrar Hussain (abrarhussainturi@gmail.com)
#
######################################################

# with context manager library

import argparse
import logging
import os
import json
import mysql

from kafka import KafkaProducer
from mysql.connector import errorcode

mysql_configs = {
  'user': 'test',
  'password': 'test',
  'host': '127.0.0.1',
  'database': 'kafka',
  'raise_on_warnings': True,
}

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Send messages to Kafka",
        epilog="Example Usage: "
               "python producer.py -c, "
               "python producer.py -f [file-path], "
               "python producer.py -d")

    parser.add_argument("-c", "--console_input",
                        dest="console_input",
                        action="store_true",
                        help="Read from console user input")

    parser.add_argument("-f", "--file", metavar="",
                        dest="file",
                        action="store",
                        help="Read from file")

    parser.add_argument("-d", "--database",
                        dest="database",
                        action="store_true",
                        help="Read data from database")

    parser.add_argument("-v", "--verbose",
                        dest="verbose",
                        action="store_true",
                        help="enable debug logging")

    return parser.parse_args()


def get_user_input():
    print('Please enter you messages. Finish with typing \'exit\' \n')

    input_list = list();
    while True:
        i = raw_input();
        if 'exit' != i:
            input_list.append(i);
        else:
            break;
    return input_list


def send_produced_messages(messages_list):
   # return
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    for message in messages_list:
        # print 'Sending message: %s' % message;
        # producer.send('test', b'%s' % message)
        # Block until a single message is sent (or timeout)
        future = producer.send('customers', json.dumps(message).encode('utf-8'))
        result = future.get(timeout=60)


def read_file(file_path):
    """
    read file and string \n
    :param file_path:
    :return: list of lines read
    """
    with open(file_path) as fp:
        lines = fp.read().splitlines()
        # remove blank lines
        return [line for line in lines if line]


def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**mysql_configs)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            print(err)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
            return;
    else:
        return conn;


def get_all_customers_names():
    conn = connect_to_mysql()
    if None != conn:
        cursor = conn.cursor()
        sql = "SELECT customer_name FROM customers"
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows;
        conn.close()


def main():
    args = parse_arguments()
    level = logging.INFO

    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    # User input
    if args.console_input:
        input_list = get_user_input()
        if input_list is not None:
            # send this to kafka
            send_produced_messages(input_list)
            # print(input_list)

    if args.file:
        lines_list = read_file(args.file)
        send_produced_messages(lines_list)

    if args.database:
        customers_name_list = list()
        rows = get_all_customers_names()

        if rows is not None:
            for r in rows:
               customers_name_list.append(r[0])
            send_produced_messages(customers_name_list)


if __name__ == "__main__":
    main()