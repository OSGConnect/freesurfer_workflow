#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import os
import sys
import logging

import psycopg2
import shutil

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'


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


def process_results():
    """
    Process results from jobs, removing any that are more than 30 days old

    :return: exit code (0 for success, non-zero for failure)
    """

    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, image_filename, state, pegasus_ts, subject " \
                "  FROM freesurfer_interface.jobs " \
                "WHERE (state = 'COMPLETED' OR" \
                "      state = 'ERROR') AND" \
                "      age(job_date) > '30 days' AND " \
                "      age(job_date) < '35 days'"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'PURGED' " \
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
            if not os.path.exists(result_dir):
                continue
            try:
                if os.path.exists(result_dir):
                    shutil.rmtree(result_dir)
                if os.path.isfile(log_filename):
                    os.unlink(log_filename)
                if os.path.isfile(output_filename):
                    os.unlink(output_filename)
                if os.path.exists(input_file):
                    os.unlink(input_file)
                    input_dir = os.path.dirname(input_file)
                    os.rmdir(input_dir)
            except OSError, e:
                logging.error("Can't remove {0} for job {1}".format(input_file,
                                                                    row[0]))
                logging.error("Exception: {0}".format(str(e)))
            finally:
                cursor.execute(job_update, [row[0]])
                conn.commit()
    except psycopg2.Error:
        logging.error("Can't connect to database")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(process_results())
