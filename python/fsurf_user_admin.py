#!/usr/bin/env python

import argparse
import sys
import getpass
import hashlib
import time

import fsurfer
import fsurfer.log

VERSION = fsurfer.__version__
PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"


def get_input(parameter, echo=True):
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
    return user_input.strip()


def add_user(args):
    """
    Process arguments and ask user for other needed parameters in order
    to add info to DB

    :param args: returned object from argparse.parse_args
    :return: exit code (0 on success, 1 on failure)
    """
    logger = fsurfer.log.get_logger()

    if args.username is None:
        username = get_input("Username")
    else:
        username = args.username
    username = username.strip()
    password = get_input("password", echo=False)
    if args.first_name is None:
        first_name = get_input("First name")
    else:
        first_name = args.first_name
    if args.last_name is None:
        last_name = get_input("Last name")
    else:
        last_name = args.last_name
    if args.email is None:
        email = get_input("Email")
    else:
        email = args.email
    if args.phone is None:
        phone = get_input("Phone")
    else:
        phone = args.phone
    if args.institution is None:
        institution = get_input("Institution")
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
        conn = fsurfer.helpers.get_db_client()
        with conn.cursor() as cursor:
            logger.info("Adding {0} to database".format(username))
            cursor.execute(user_insert, (username,
                                         first_name,
                                         last_name,
                                         email,
                                         institution,
                                         phone,
                                         password,
                                         salt))
            if cursor.rowcount != 1:
                sys.stderr.write("{0}".format(cursor.statusmessage))
                logger.error("Encountered error while adding" +
                             "user {0}: {1}".format(username, cursor.statusmessage))
                return 1
        logger.info("User {0} added".format(username))
        conn.commit()
        conn.close()
        return 0
    except Exception as e:
        sys.stderr.write("Got exception: {0}".format(e))
        logger.exception("Got exception: {0}".format(e))
        return 1


def disable_user(args):
    """
    Process arguments and ask user for other needed parameters in order
    to disable an user account

    :param args: returned object from argparse.parse_args
    :return: exit code (0 on success, 1 on failure)
    """
    logger = fsurfer.log.get_logger()

    if args.username is None:
        username = get_input("username")
    else:
        username = args.username
    username = username.strip()
    user_disable = "UPDATE freesurfer_interface.users " \
                   "SET password = 'xxx', salt = 'xxx' " \
                   "WHERE username = %s"
    try:
        conn = fsurfer.helpers.get_db_client()
        with conn.cursor() as cursor:
            cursor.execute(user_disable, [username])
            if cursor.rowcount != 1:
                sys.stderr.write("{0}\n".format(cursor.statusmessage))
                logger.error("Got pgsql error: {0}".format(cursor.statusmessage))
                return 1
        conn.commit()
        logger.info("Disabled user {0}".format(username))
        conn.close()
        return 0
    except Exception as e:
        sys.stderr.write("Got exception: {0}\n".format(e))
        logger.exception("Got exception: {0}".format(e))
        return 1


def modify_user(args):
    """
    Process arguments and ask user for other needed parameters in order
    to change a fsurfer user password

    :param args: returned object from argparse.parse_args
    :return: exit code (0 on success, 1 on failure)
    """
    logger = fsurfer.log.get_logger()

    if args.username is None:
        username = get_input("username")
    else:
        username = args.username
    username = username.strip()
    password = get_input("password", echo=False)
    salt = hashlib.sha256(str(time.time())).hexdigest()
    password = hashlib.sha256(salt + password).hexdigest()

    user_update = "UPDATE freesurfer_interface.users " \
                  "SET password = %s, salt = %s " \
                  "WHERE username = %s"
    try:
        conn = fsurfer.helpers.get_db_client()
        logger.info("Updating password for {0}".format(username))
        with conn.cursor() as cursor:
            cursor.execute(user_update, (password,
                                         salt,
                                         username))
            if cursor.rowcount != 1:
                logger.error("Got pgsql error: {0}".format(cursor.statusmessage))
                sys.stderr.write("{0}\n".format(cursor.statusmessage))
                return 1
        conn.commit()
        logger.info("Password updated")
        conn.close()
        return 0
    except Exception as e:
        sys.stderr.write("Got exception: {0}\n".format(e))
        logger.exception("Got exception: {0}".format(e))
        return 1


def list_users(args):
    """
    List current users in fsurf database and current status

    :param args: needed for function signature but unused
    :return: exit code (0 on success, 1 on failure)
    """
    logger = fsurfer.log.get_logger()

    user_query = "SELECT username, salt " \
                 "FROM freesurfer_interface.users"
    try:
        conn = fsurfer.helpers.get_db_client()
        with conn.cursor() as cursor:
            cursor.execute(user_query)
            sys.stdout.write("{0:30} {1:20}\n".format('User',
                                                      'Status'))
            for row in cursor.fetchall():
                status = 'Enabled'
                if 'xxx' in row[1]:
                    status = 'Disabled'
                sys.stdout.write("{0:30} {1:20}\n".format(row[0], status))

        conn.commit()
        logger.info("Password updated")
        conn.close()
        return 0
    except Exception as e:
        sys.stderr.write("Got exception: {0}\n".format(e))
        logger.exception("Got exception: {0}".format(e))
        return 1


def main():
    """
    Process arguments and ask user for other needed parameters in order
    to add info to DB

    :return: exit code (0 on success, 1 on failure)
    """
    fsurfer.log.initialize_logging()
    parser = argparse.ArgumentParser(description='Manage fsurf user accounts')
    subparsers = parser.add_subparsers(title='commands',
                                       description='actions that can be taken')
    parser.add_argument('--version', action='version',
                        version='%(prog)s ' + VERSION)
    parser.add_argument('--config', dest='config_file', default=PARAM_FILE_LOCATION,
                        help='location of file with configuration')
    # create subparser for list action
    list_parser = subparsers.add_parser('list',
                                        help='List users')
    list_parser.set_defaults(func=list_users)

    # create subparser for status action
    create_parser = subparsers.add_parser('create',
                                          help='Create a new user account')
    create_parser.add_argument('--username', dest='username', default=None,
                               help='username')
    create_parser.add_argument('--first-name', dest='first_name', default=None,
                               help='firstname')
    create_parser.add_argument('--last-name', dest='last_name', default=None,
                               help='lastname')
    create_parser.add_argument('--email', dest='email', default=None,
                               help='email')
    create_parser.add_argument('--phone', dest='phone', default=None,
                               help='phone')
    create_parser.add_argument('--institution', dest='institution', default=None,
                               help='institution')
    create_parser.set_defaults(func=add_user)

    # create subparser for disable action
    disable_parser = subparsers.add_parser('disable',
                                           help='Disable specified user')
    disable_parser.add_argument('--username', dest='username', default=None,
                                help='Username to disable')
    disable_parser.set_defaults(func=disable_user)

    # create subparser for modify action
    modify_parser = subparsers.add_parser('modify',
                                          help='Change password for specified user')
    modify_parser.add_argument('--username', dest='username', default=None,
                               help='Username to modify')
    modify_parser.set_defaults(func=modify_user)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)

if __name__ == '__main__':
    sys.exit(main())
