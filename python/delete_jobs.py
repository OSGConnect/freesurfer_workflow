#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import argparse
import os
import re
import shutil
import subprocess
import sys
import time

import psycopg2

import fsurfer
import fsurfer.helpers
import fsurfer.log

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
VERSION = fsurfer.__version__


def purge_workflow_file(path):
    """
    Remove the results in specified directory

    :param path: path to directory or file to delete
    :return: True if successfully removed, False otherwise
    """
    logger = fsurfer.log.get_logger()
    if not os.path.exists(path):
        return True
    try:
        if os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path):
            os.rmdir(path)
        return True
    except OSError as e:
        logger.exception("Exception: {0}".format(str(e)))
        return False


def delete_job():
    """
    Delete all jobs in a delete pending state, stopping pegasus
    workflows if needed

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
    args = parser.parse_args(sys.argv[1:])
    if args.dry_run:
        sys.stdout.write("Doing a dry run, no changes will be made\n")

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, image_filename, state, pegasus_ts, subject " \
                "FROM freesurfer_interface.jobs " \
                "WHERE state = 'DELETE PENDING'"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'DELETED' " \
                 "WHERE id = %s;"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            logger.info("Deleting workflow {0} for user {1}".format(row[0],
                                                                    row[1]))
            username = row[1]
            # pegasus_ts is stored as datetime in the database, convert it to what we have on the fs
            pegasus_ts = row[4]
            workflow_dir = os.path.join(FREESURFER_BASE,
                                        username,
                                        'workflows',
                                        'fsurf',
                                        'pegasus',
                                        'freesurfer',
                                        pegasus_ts)
            result_dir = os.path.join(FREESURFER_BASE,
                                      username,
                                      'workflows',
                                      'output',
                                      'fsurf',
                                      'pegasus',
                                      'freesurfer',
                                      pegasus_ts)
            if args.dry_run:
                sys.stdout.write("Would run pegasus-remove "
                                 "{0}\n".format(result_dir))
            else:
                try:
                    output = subprocess.check_output(['/usr/bin/pegasus-remove',
                                                      workflow_dir],
                                                     stderr=subprocess.STDOUT)
                    exit_code = 0
                except subprocess.CalledProcessError as err:
                    exit_code = err.returncode
                    output = err.output
                # job removed (code = 0) just now  or it's been removed earlier
                if exit_code == 0 or 'not found' in output:
                    # look for condor job id and wait a bit for pegasus to remove it
                    # so that we can delete the pegasus directories
                    job_id = re.match(r'Job (\d+.\d+) marked for removal', output)
                    if job_id is not None:
                        logger.info("Waiting for running jobs to be removed...\n")
                        count = 0
                        while True:
                            time.sleep(10)
                            try:
                                output = subprocess.check_output(["/usr/bin/condor_q",
                                                                  job_id.group(1)])
                            except subprocess.CalledProcessError:
                                logger.exception("An error occurred while "
                                                 "checking for running "
                                                 "jobs, exiting...\n")
                                break
                            if 'pegasus-dagman' not in output:
                                break
                            count += 1
                            if count > 30:
                                logger.error("Can't remove job, exiting...\n")
                                break
            logger.info("Jobs removed, removing workflow directory\n")
            try:
                if not args.dry_run:
                    shutil.rmtree(workflow_dir)
            except shutil.Error:
                logger.exception("Can't remove directory at "
                                 "{0}, exiting...\n".format(workflow_dir))

            deletion_list = []
            # add input file
            deletion_list.append(os.path.join(FREESURFER_BASE, username, 'input', row[2]))
            # remove files in result dir
            for entry in os.listdir(result_dir):
                deletion_list.append(os.path.join(result_dir, entry))
            deletion_list.append(result_dir)
            # delete output and log copied over after workflow completion
            # if present
            deletion_list.append(os.path.join(FREESURFER_BASE,
                                              username,
                                              'results',
                                              'recon_all-{0}.log'.format(row[0])))
            deletion_list.append(os.path.join(FREESURFER_BASE,
                                              username,
                                              'results',
                                              "{0}_{1}_output.tar.bz2".format(row[0],
                                                                              row[5])))
            for entry in deletion_list:
                if args.dry_run:
                    sys.stdout.write("Would delete {0}\n".format(entry))
                else:
                    logger.info("Removing {0}".format(entry))
                    if not purge_workflow_file(entry):
                        logger.error("Can't remove {0} for job {1}".format(entry,
                                                                           row[0]))
            logger.info("Setting workflow {0} to DELETED".format(row[0]))
            cursor.execute(job_update, [row[0]])
            if args.dry_run:
                conn.rollback()
            else:
                conn.commit()
    except psycopg2.Error as e:
        logger.exception("Error: {0}".format(e))
        return 1
    finally:
        conn.commit()
        conn.close()
    return 0


if __name__ == '__main__':
    # workaround missing subprocess.check_output
    if "check_output" not in dir(subprocess):  # duck punch it in!
        def check_output(*popenargs, **kwargs):
            """
            Run command with arguments and return its output as a byte string.

            Backported from Python 2.7 as it's implemented as pure python
            on stdlib.

            """
            process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
            output, unused_err = process.communicate()
            retcode = process.poll()
            if retcode:
                cmd = kwargs.get("args")
                if cmd is None:
                    cmd = popenargs[0]
                error = subprocess.CalledProcessError(retcode, cmd)
                error.output = output
                raise error
            return output

        subprocess.check_output = check_output

    sys.exit(delete_job())
