#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import argparse
import os
import sys
import logging

import psycopg2
import shutil

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
VERSION = '1.3.2'


def get_db_parameters():
    """
    Read database parameters from a file and return it

    :return: a tuple of (database_name, user, password, hostname)
    """
    parameters = {}
    with open(PARAM_FILE_LOCATION) as param_file:
        for line in param_file:
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
    return psycopg2.connect(database=db, user=user, host=host, password=password)


def purge_workflow_files(result_dir, log_filename, input_file, output_filename):
    """
    Remove the results in specified directory

    :param result_dir: path to directory with workflow outputs
    :param log_filename: path to log file for workflow
    :param input_file: path to input for workflow
    :param output_filename: path to mgz output file for workflow
    :return: True if successfully removed, False otherwise
    """
    if not os.path.exists(result_dir):
        return True
    try:

        shutil.rmtree(result_dir)
        if os.path.isfile(log_filename):
            os.unlink(log_filename)
        if os.path.isfile(output_filename):
            os.unlink(output_filename)
        if os.path.exists(input_file):
            os.unlink(input_file)
            input_dir = os.path.dirname(input_file)
            os.rmdir(input_dir)
        return True
    except OSError, e:
        logging.error("Exception: {0}".format(str(e)))
        return False


def process_results():
    """
    Process results from jobs, removing any that are more than 30 days old

    :return: exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(description="Process and remove old results")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments for action
    parser.add_argument('--dry-run', dest='dry_run',
                        action='store_true', default=False,
                        help='Mock actions instead of carrying them out')
    args = parser.parse_args(sys.argv[1:])
    if args.dry_run:
        sys.stdout.write("Doing a dry run, no changes will be made\n")

    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, image_filename, state, pegasus_ts, subject " \
                "  FROM freesurfer_interface.jobs " \
                "WHERE (state = 'COMPLETED' OR" \
                "      state = 'ERROR') AND" \
                "      age(job_date) > '30 days';"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'DELETED' " \
                 "WHERE id = %s;"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            username = row[1]
            result_dir = os.path.join(FREESURFER_BASE,
                                      username,
                                      'workflows',
                                      'output',
                                      'fsurf',
                                      'pegasus',
                                      'freesurfer',
                                      row[4])
            input_file = os.path.join(FREESURFER_BASE, username, 'input', row[2])
            log_filename = os.path.join(FREESURFER_BASE,
                                        username,
                                        'results',
                                        'recon_all-{0}.log'.format(row[0]))
            output_filename = os.path.join(FREESURFER_BASE,
                                           username,
                                           'results',
                                           "{0}_{1}_output.tar.bz2".format(row[0],
                                                                           row[5]))
            if args.dry_run:
                sys.stdout.write("Would delete {0}\n".format(result_dir))
                sys.stdout.write("Would delete {0}\n".format(input_file))
                sys.stdout.write("Would delete {0}\n".format(log_filename))
                sys.stdout.write("Would delete {0}\n".format(output_filename))
                continue
            if not purge_workflow_files(result_dir,
                                        log_filename,
                                        input_file,
                                        output_filename):
                logging.error("Can't remove {0} for job {1}".format(input_file,
                                                                    row[0]))
                continue
            cursor.execute(job_update, [row[0]])
            conn.commit()
    except psycopg2.Error, e:
        logging.error("Error: {0}".format(e))
        return 1
    finally:
        conn.commit()
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(process_results())
