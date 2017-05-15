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

PARAM_FILE_LOCATION = "/etc/fsurf/db_info"
VERSION = fsurfer.__version__


def purge_workflow_files(result_dir, log_filename, output_filename):
    """
    Remove the results in specified directory

    :param result_dir: path to directory with workflow outputs
    :param log_filename: path to log file for workflow
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
        return True
    except OSError, e:
        logger.error("Exception: {0}".format(str(e)))
        return False


def remove_input_file(input_file):
    """
    Remove input file

    :param input_file: path to input file to remove
    :return: True if successful, False otherwise
    """
    logger = fsurfer.log.get_logger()
    if not os.path.exists(input_file):
        return True
    try:
        os.unlink(input_file)
        logger.info("Unlinked file")
        return True
    except OSError as e:
        logger.exception("Exception: {0}".format(str(e)))
        return False


def remove_input_directory(input_dir):
    """
    Remove input dir

    :param input_dir: path to input directory to remove
    :return: True if successful, False otherwise
    """
    logger = fsurfer.log.get_logger()
    if not os.path.exists(input_dir):
        return True
    try:
        input_dir = os.path.dirname(input_dir)
        os.rmdir(input_dir)
        logger.info("Removed directory {0}".format(input_dir))
        return True
    except OSError as e:
        logger.exception("Exception: {0}".format(str(e)))
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
    job_query = "SELECT jobs.id, " \
                "       jobs.username," \
                "       jobs.state," \
                "       job_run.pegasus_ts," \
                "       jobs.subject " \
                "FROM freesurfer_interface.jobs AS jobs, " \
                "     freesurfer_interface.job_run AS job_run " \
                "WHERE (state = 'COMPLETED' OR " \
                "      state = 'ERROR') AND " \
                "      age(job_date) > '30 days' AND " \
                "      jobs.id = job_run.job_id"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'DELETED' " \
                 "WHERE id = %s;"
    input_update = "UPDATE freesurfer_interface.input_files " \
                   "SET purged = TRUE  " \
                   "WHERE id = %s"
    input_select = "SELECT id, path, filename " \
                   "FROM freesurfer_interface.input_files " \
                   "WHERE NOT purged AND job_id = %s"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            workflow_id = row[0]
            username = row[1]
            pegasus_ts = row[4]
            logger.info("Processing workflow {0} for user {1}".format(workflow_id,
                                                                      username))
            result_dir = os.path.join(fsurfer.FREESURFER_BASE,
                                      username,
                                      'workflows',
                                      'output',
                                      'fsurf',
                                      'pegasus',
                                      'freesurfer',
                                      pegasus_ts)
            log_filename = os.path.join(fsurfer.FREESURFER_BASE,
                                        username,
                                        'results',
                                        'recon_all-{0}.log'.format(workflow_id))
            output_filename = os.path.join(fsurfer.FREESURFER_BASE,
                                           username,
                                           'results',
                                           "{0}_{1}_output.tar.bz2".format(workflow_id,
                                                                           row[4]))
            cursor2 = conn.cursor()
            cursor2.execute(input_select, [workflow_id])
            file_removal_error = False
            input_directory = None
            for input_row in cursor2.fetchall():
                input_file = input_row[1]
                input_directory = os.path.dirname(input_file)
                logger.info("Deleting file {0}".format(input_file))
                if args.dry_run:
                    sys.stdout.write("Would delete {0}\n".format(input_file))
                    continue
                if not os.path.exists(input_file):
                    logger.info("File not present")
                    continue
                if not remove_input_file(input_file):
                    file_removal_error = True
                    logger.error("Can't remove {0} for job {1}".format(input_file,
                                                                       workflow_id))
                cursor3 = conn.cursor()
                cursor3.execute(input_update, [input_row[0]])

            if not file_removal_error and input_directory:
                if args.dry_run:
                    sys.stdout.write("Would delete directory {0}\n".format(input_directory))
                if not remove_input_directory(input_directory):
                    logger.error("Can't remove {0} for job {1}".format(input_directory,
                                                                        workflow_id))

            if args.dry_run:
                sys.stdout.write("Would delete {0}\n".format(result_dir))
                sys.stdout.write("Would delete {0}\n".format(log_filename))
                sys.stdout.write("Would delete {0}\n".format(output_filename))
                continue
            logger.info("Removing {0}, {1}, {2}".format(result_dir,
                                                             log_filename,
                                                             output_filename))
            if not purge_workflow_files(result_dir,
                                        log_filename,
                                        output_filename):
                logger.error("Can't remove files for job {1}".format(workflow_id))
                continue
            logger.info("Setting workflow {0} to DELETED".format(workflow_id))
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
