#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import os
import sys
import logging

import psycopg2

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


def process_inputs():
    """
    Process uploaded images, removing those older than 2 weeks

    :return: exit code (0 for success, non-zero for failure)
    """

    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, image_filename, state FROM freesurfer_interface.jobs " \
                "WHERE age(job_date) > '21 days' AND " \
                "      age(job_date) < '22 days'"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = %s " \
                 "WHERE id = %s;"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            input_file = os.path.join(FREESURFER_BASE, row[1], 'input', row[2])
            if not os.path.exists(input_file):
                continue
            try:
                os.unlink(input_file)
                input_dir = os.path.dirname(input_file)
                os.rmdir(input_dir)
            except OSError, e:
                logging.error("Can't remove {0} for job {1}".format(input_file,
                                                                    row[0]))
                logging.error("Exception: {0}".format(str(e)))
            if row[3].upper() == 'UPLOADED':
                cursor.execute(job_update, ['ERROR', row[0]])
                conn.commit()
                return 1
            conn.commit()
            conn.close()
    except psycopg2.Error:
        logging.error("Can't connect to database")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(process_inputs())
