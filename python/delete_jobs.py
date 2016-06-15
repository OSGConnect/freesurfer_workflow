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
    except OSError as e:
        log.exception("Exception: {0}".format(str(e)))
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
            pegasus_ts = row[4].strftime('%Y%m%dT%H%M%S%z')
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
                                                      result_dir],
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
