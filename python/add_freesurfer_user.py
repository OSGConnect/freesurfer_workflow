#!/usr/bin/env python

import argparse
import sys
import getpass
import hashlib
import time

import psycopg2

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"

def get_db_parameters():
    """
    Read database parameters from a file and return it

    :return: a tuple of (database_name, user, password, hostname)
    """
    parameters = {}
    with open(PARAM_FILE_LOCATION) as param_file:
        line = param_file.readline()
        key, val = line.strip().split('=')
        parameters[key.strip()] = val.strip()
    return (parameters['database'],
            parameters['user'],
            parameters['password'],
            parameters['hostname'])


def get_db_client():
    """
    Get a postgresql client instance and return it

    :return: a redis client instance or None if failure occurs
    """
    db, user, password, host = get_db_parameters()
    return psycopg2.connect(database=db, user=user, host=host)


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
    password = query_user("password", echo=False)
    if args.first_name is None:
        first_name = query_user("first_name")
    else:
        first_name = args.first_name
    if args.last_name is None:
        last_name = query_user("last_name")
    else:
        last_name = args.last_name
    if args.email is None:
        email = query_user("email")
    else:
        email = args.email
    if args.phone is None:
        phone = query_user("phone")
    else:
        phone = args.phone
    if args.institution is None:
        institution = query_user("institution")
    else:
        institution = args.institution
    salt = hashlib.sha256(str(time.time())).hexdigest()
    password = hashlib.sha256(salt + password).hexdigest()

    user_insert = "INSERT INTO freesurfer_interface.users(username," \
                  "                                       first_name," \
                  "                                       last_name," \
                  "                                       email," \
                  "                                       institution," \
                  "                                       phone," \
                  "                                       password," \
                  "                                       salt) " \
                  "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        conn = get_db_client()
        with conn.cursor() as cursor:
            cursor.execute(user_insert, (username,
                                         first_name,
                                         last_name,
                                         email,
                                         institution,
                                         phone,
                                         password,
                                         salt))
            if cursor.statusmessage != 'INSERT 0 1':
                return 1
        conn.close()
        return 0
    except Exception:
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse request and act appropriately')
    parser.add_argument('--username', dest='username', default=None,
                        help='username')
    parser.add_argument('--first-name', dest='first_name', default=None,
                        help='firstname')
    parser.add_argument('--last-name', dest='last_name', default=None,
                        help='lastname')
    parser.add_argument('--email', dest='email', default=None,
                        help='email')
    parser.add_argument('--phone', dest='phone', default=None,
                        help='phone')
    parser.add_argument('--institution', dest='institution', default=None,
                        help='institution')
    parser.add_argument('--dbparams', dest='db_param_file', default=PARAM_FILE_LOCATION,
                        help='location of file with database information')
    args = parser.parse_args(sys.argv[1:])
    sys.exit(main(args))
