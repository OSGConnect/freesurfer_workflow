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


def delete_workflow(args):
    """
    Remove a job from being processed

    :param args: argparse parsed args
    :return: a tuple with response_body, status
    """
    if args.id is None:
        sys.stdout.write("Job id not given, exiting...\n")
        sys.exit(1)
    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT state FROM freesurfer_interface.jobs  " \
                "WHERE id = %s;"
    try:
        cursor.execute(job_query, [args.id])
        if cursor.rowcount != 1:
            sys.stderr.write("Workflow {0} not found\n".format(args.id))
            return 1
        row = cursor.fetchone()
        if row:
            state = row[0]
        else:
            state = 'None'
        if state not in ['RUNNING', 'QUEUED', 'FAILED', 'COMPLETED']:
            sys.stderr.write("Workflow has already been marked for "
                             "deletion or has been deleted\n")
            return 1
        else:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'DELETE PENDING'" \
                         "WHERE id = %s;"
            cursor.execute(job_update, [args.id])
            if cursor.rowcount != 1:
                conn.rollback()
                sys.stderr.write("Workflow not found or could not "
                                 "change workflow state\n")
                sys.exit(1)
            conn.commit()
            sys.stdout.write("Workflow {0} marked for deletion".format(args.id))
    except Exception as e:
        conn.rollback()
        sys.stderr.write("Exception caught: {0}".format(e))
        return 1
    finally:
        conn.close()
    return 0


def list_workflows(args):
    """
    Get status for all jobs submitted by user in last week

    :param args: argparse parsed args
    :return: a tuple with response_body, status
    """
    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    accounting_query = "SELECT tasks_completed, tasks " \
                       "FROM  freesurfer_interface.job_run  " \
                       "WHERE job_id = %s"
    if args.all_workflows:
        job_query = "SELECT id, " \
                    "       subject, " \
                    "       state, " \
                    "       date_trunc('seconds', job_date), " \
                    "       multicore," \
                    "       username " \
                    "FROM freesurfer_interface.jobs " \
                    "WHERE purged IS NOT TRUE "
        if args.username:
            job_query += " AND username = %s"
        job_query += "ORDER BY job_date DESC;"
    else:
        job_query = "SELECT id, " \
                    "       subject, " \
                    "       state, " \
                    "       date_trunc('seconds', job_date), " \
                    "       multicore," \
                    "       username " \
                    "FROM freesurfer_interface.jobs " \
                    "WHERE purged IS NOT TRUE AND " \
                    "      age(job_date) < '1 week' "
        if args.username:
            job_query += " AND username = %s"
        job_query += "ORDER BY job_date DESC;"
    try:
        if args.username:
            cursor.execute(job_query, [args.username])
        else:
            cursor.execute(job_query, [])
        if cursor.rowcount == 0:
            sys.stdout.write("\nNo workflows present\n")
            return 0
        sys.stdout.write("{0:10} {1:10} {2:27} ".format('Subject',
                                                        'Workflow',
                                                        'Submit time (Central Time)'))
        sys.stdout.write("{0:10} {1:15} {2:10} {3:10}\n".format('Cores',
                                                                'Status',
                                                                'Tasks completed',
                                                                'Username'))
        for row in cursor.fetchall():
            cursor2 = conn.cursor()
            cursor2.execute(accounting_query, [row[0]])
            result = cursor2.fetchone()
            if result is None:
                completion = 'N/A'
            else:
                completion = '{0}/{1}'.format(result[0], result[1])
            sys.stdout.write("{0:10} {1:<10} {2:<27} ".format(row[1],
                                                              row[0],
                                                              str(row[3])))
            sys.stdout.write("{0:<10} {1:<15} {2:<10} {3:<10}\n".format(8 if row[4] else 2,
                                                                        row[2],
                                                                        completion,
                                                                        row[5]))
    except Exception, e:
        sys.stderr.write("Exception caught: {0}".format(e))
        return 1
    finally:
        conn.commit()
        conn.close()

    return 0


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
    list_parser = subparsers.add_parser('list', help='List workflows')
    list_parser.add_argument('--username', dest='username', default=None,
                             help='Limit workflows to specified username')
    list_parser.add_argument('--all-workflows',
                             dest='all_workflows',
                             action='store_true',
                             default=False,
                             help='List all workflows')
    list_parser.set_defaults(func=list_workflows)

    # create subparser for delete action
    modify_parser = subparsers.add_parser('delete',
                                          help='Delete specified workflow')
    modify_parser.add_argument('--id', dest='id', default=None,
                               type=int,
                               help='workflow id')
    modify_parser.set_defaults(func=delete_workflow)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)

if __name__ == '__main__':
    sys.exit(main())
