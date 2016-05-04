#!/usr/bin/env python

import argparse
import sys
import getpass
import hashlib
import time

import psycopg2

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"


def get_db_parameters(config_file=None):
    """
    Read database parameters from a file and return it

    :param config_file: location of file with database parameters
    :return: a tuple of (database_name, user, password, hostname)
    """
    parameters = {}
    if config_file is None:
        config_file = PARAM_FILE_LOCATION
    with open(config_file) as param_file:
        for line in param_file.readlines():
            key, val = line.strip().split('=')
            parameters[key.strip()] = val.strip()
    return (parameters['database'],
            parameters['user'],
            parameters['password'],
            parameters['hostname'])


def get_db_client(config_file=None):
    """
    Get a postgresql client instance and return it

    :param config_file: location of file with database parameters
    :return: a redis client instance or None if failure occurs
    """
    db, user, password, host = get_db_parameters(config_file)
    return psycopg2.connect(database=db, user=user, host=host, password=password)


def query_user(parameter, echo=True):
    """
    Query user for specified parameter and return it

    :param parameter: name of item to ask the user for
    :param echo: boolean to indicate whether to echo what the user types or not
    :return: user input
    """
    user_input = ""
    while user_input == "":
        if echo:
            user_input = raw_input("Please enter {0}: ".format(parameter))
        else:
            user_input = getpass.getpass("Please enter {0}: ".format(parameter))
    return user_input


def main(args):
    """
    Process arguments and ask user for other needed parameters in order
    to add info to DB

    :param args: returned object from argparse.parse_args
    :return: exit code (0 on success, 1 on failure)
    """
    if args.username is None:
        username = query_user("username")
    else:
        username = args.username

    user_insert = "UPDATE freesurfer_interface.users " \
                  "SET password = 'xxx', salt = 'xxx' " \
                  "WHERE username = %s"
    try:
        conn = get_db_client(args.db_param_file)
        with conn.cursor() as cursor:
            cursor.execute(user_insert, username)
            if cursor.rowcount != 1:
                sys.stderr.write("{0}\n".format(cursor.statusmessage))
                return 1
        conn.commit()
        conn.close()
        return 0
    except Exception, e:
        sys.stderr.write("Got exception: {0}\n".format(e))
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Disable a fsurf user account')
    parser.add_argument('--username', dest='username', default=None,
                        help='username')
    parser.add_argument('--dbparams', dest='db_param_file', default=PARAM_FILE_LOCATION,
                        help='location of file with database information')
    args = parser.parse_args(sys.argv[1:])
    sys.exit(main(args))
