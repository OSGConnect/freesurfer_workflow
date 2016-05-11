#!/usr/bin/env python

import argparse
import sys
import getpass
import hashlib
import time

import fsurfer

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"


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
    salt = hashlib.sha256(str(time.time())).hexdigest()
    password = hashlib.sha256(salt + password).hexdigest()

    user_insert = "UPDATE freesurfer_interface.users " \
                  "SET password = %s, salt = %s " \
                  "WHERE username = %s"
    try:
        conn = fsurfer.helpers.get_db_client()

        with conn.cursor() as cursor:
            cursor.execute(user_insert, (password,
                                         salt,
                                         username))
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
    parser = argparse.ArgumentParser(description='Change password for a fsurf users')
    parser.add_argument('--username', dest='username', default=None,
                        help='username')
    parser.add_argument('--dbparams', dest='db_param_file', default=PARAM_FILE_LOCATION,
                        help='location of file with database information')
    args = parser.parse_args(sys.argv[1:])
    sys.exit(main(args))
