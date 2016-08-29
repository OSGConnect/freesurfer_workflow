#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import argparse
import os
import sys

import psycopg2

import fsurfer
import fsurfer.helpers
import fsurfer.log

FREESURFER_BASE = '/stash2/user/fsurf/'
VERSION = fsurfer.__version__


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


def process_inputs():
    """
    Process uploaded images, removing those older than 2 weeks

    :return: exit code (0 for success, non-zero for failure)
    """

    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, state FROM freesurfer_interface.jobs " \
                "WHERE age(job_date) > '21 days' " \
                "AND STATE NOT IN ('FAILED', 'DELETED', 'ERROR')"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = %s " \
                 "WHERE id = %s;"
    input_update = "UPDATE freesurfer_interface.input_files " \
                   "SET state = %s  " \
                   "WHERE id = %s"
    input_select = "SELECT id, path, filename " \
                   "FROM freesurfer_interface.input_files " \
                   "WHERE state IS NOT 'PURGED' AND job_id = %s"
    parser = argparse.ArgumentParser(description="Process and remove old inputs")
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
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            logger.info("Processing workflow {0} for user {1} ".format(row[0],
                                                                       row[1])+
                        "in state {0}".format(row[3]))
            cursor2 = conn.cursor()
            cursor2.execute(input_select, [row[0]])
            file_removal_error = False
            input_directory = None
            for input_row in cursor2.fetchall():
                input_file = os.path.join(input_row[1], input_row[2])
                input_directory = os.path.dirname(input_file)
                logger.info("Deleting file {0}".format(input_file))
                if args.dry_run:
                    sys.stdout.write("Would delete {0} and directory\n".format(input_file))
                    continue
                if not os.path.exists(input_file):
                    logger.info("File not present")
                    continue
                if not remove_input_file(input_file):
                    file_removal_error = True
                    logger.error("Can't remove {0} for job {1}".format(input_file,
                                                                       row[0]))
            if not file_removal_error:
                cursor3 = conn.cursor()
                cursor3.execute(input_update, ['PURGED', input_row[0]])
                if not remove_input_directory(input_directory):
                    logger.error("Can't remove {0} for job {1}".format(input_directory,
                                                                        row[0]))
            if row[3].upper() == 'UPLOADED':
                cursor.execute(job_update, ['ERROR', row[0]])
                conn.commit()
                logger.info("Changed workflow {0} status ".format(row[0]) +
                            "to ERROR")
                return 1
            conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(process_inputs())
