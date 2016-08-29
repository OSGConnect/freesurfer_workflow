#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import argparse
import os
import sys

import psycopg2
import shutil

import fsurfer
import fsurfer.log
import fsurfer.helpers

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
VERSION = fsurfer.__version__


def purge_workflow_files(result_dir, log_filename, input_file, output_filename):
    """
    Remove the results in specified directory

    :param result_dir: path to directory with workflow outputs
    :param log_filename: path to log file for workflow
    :param input_file: path to input for workflow
    :param output_filename: path to mgz output file for workflow
    :return: True if successfully removed, False otherwise
    """
    logger = fsurfer.log.get_logger()
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
        logger.error("Exception: {0}".format(str(e)))
        return False


def process_results():
    """
    Process results from jobs, removing any that are more than 30 days old

    :return: exit code (0 for success, non-zero for failure)
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    parser = argparse.ArgumentParser(description="Process and remove old results")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments for action

    parser.add_argument('--dry-run', dest='dry_run',
                        action='store_true', default=False,
                        help='Mock actions instead of carrying them out')
    parser.add_argument('--debug', dest='debug',
                        action='store_true', default=False,
                        help='Output debug messages')
    args = parser.parse_args(sys.argv[1:])
    if args.debug:
        fsurfer.log.set_debugging()
    if args.dry_run:
        sys.stdout.write("Doing a dry run, no changes will be made\n")

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, state, pegasus_ts, subject " \
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
            logger.info("Processing workflow {0} for user {1}".format(row[0],
                                                                      row[1]))
            username = row[1]
            # pegasus_ts is stored as datetime in the database, convert it to what we have on the fs
            pegasus_ts = row[4]
            result_dir = os.path.join(FREESURFER_BASE,
                                      username,
                                      'workflows',
                                      'output',
                                      'fsurf',
                                      'pegasus',
                                      'freesurfer',
                                      pegasus_ts)
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
            logger.info("Removing {0}, {1}, {2}, {3}".format(result_dir,
                                                             input_file,
                                                             log_filename,
                                                             output_filename))
            if not purge_workflow_files(result_dir,
                                        log_filename,
                                        input_file,
                                        output_filename):
                logger.error("Can't remove {0} for job {1}".format(input_file,
                                                                   row[0]))
                continue
            logger.info("Setting workflow {0} to DELETED".format(row[0]))
            cursor.execute(job_update, [row[0]])
            conn.commit()
    except psycopg2.Error as e:
        logger.error("Error: {0}".format(e))
        return 1
    finally:
        conn.commit()
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(process_results())
