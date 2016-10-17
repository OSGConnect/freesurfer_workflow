#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
import shutil
import xml
import parser
import re
import datetime
from email.mime.text import MIMEText

import psycopg2
import fsurfer

VERSION = fsurfer.__version__

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


def update_completed_tasks(jobid):
    """
    Email user informing them that a workflow has completed

    :param jobid: id for workflow
    :return: None
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    try:
        conn = get_db_client()
        cursor = conn.cursor()
        logger.info("Incrementing tasks completd for workflow {0}".format(jobid))

        run_update = "UPDATE freesurfer_interface.job_run  " \
                     "SET tasks_completed = tasks_completed + 1 " \
                     "WHERE job_id = %s;"
        cursor.execute(run_update, [jobid])
        conn.commit()
        conn.close()
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return


def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Update fsurf job info and "
                                                 "email user about job "
                                                 "completion")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments identifying workflow
    parser.add_argument('--id', dest='workflow_id',
                        action='store', help='Pegasus workflow id to use')

    args = parser.parse_args(sys.argv[1:])
    update_completed_tasks(args.workflow_id)

    sys.exit(0)

if __name__ == '__main__':
    main()
