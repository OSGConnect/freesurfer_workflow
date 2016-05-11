#!/usr/bin/env python

import argparse
import sys
import getpass
import hashlib
import time

import fsurfer
import fsurfer.logging

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
        username = query_user("Username")
    else:
        username = args.username
    password = query_user("password", echo=False)
    if args.first_name is None:
        first_name = query_user("First name")
    else:
        first_name = args.first_name
    if args.last_name is None:
        last_name = query_user("Last name")
    else:
        last_name = args.last_name
    if args.email is None:
        email = query_user("Email")
    else:
        email = args.email
    if args.phone is None:
        phone = query_user("Phone")
    else:
        phone = args.phone
    if args.institution is None:
        institution = query_user("Institution")
    else:
        institution = args.institution
    salt = hashlib.sha256(str(time.time())).hexdigest()
    password = hashlib.sha256(salt + password).hexdigest()
    logger = fsurfer.logging.get_logger()
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
        conn = fsurfer.helpers.get_db_client()
        with conn.cursor() as cursor:
            logger.info("Adding {0} to database\n".format(username))
            cursor.execute(user_insert, (username,
                                         first_name,
                                         last_name,
                                         email,
                                         institution,
                                         phone,
                                         password,
                                         salt))
            if cursor.rowcount != 1:
                sys.stderr.write("{0}\n".format(cursor.statusmessage))
                logger.error("Encountered error while adding" +
                             "user {0}: {1}\n".format(username, cursor.statusmessage))
                return 1
        logger.info("User added")
        conn.commit()
        conn.close()
        return 0
    except Exception, e:
        sys.stderr.write("Got exception: {0}\n".format(e))
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a fsurf user account')
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
