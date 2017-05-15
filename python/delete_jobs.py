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

PARAM_FILE_LOCATION = "/etc/fsurf/db_info"
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


def get_input_files(workflow_id):
    """
    Get a list of input files and return this as a list

    :param workflow_id:  id for workflow
    :return: a list of input files for specified id
    """
    logger = fsurfer.log.get_logger()
    input_files = []
    try:
        conn = fsurfer.helpers.get_db_client()
        cursor = conn.cursor()
        input_query = "SELECT path " \
                      "FROM freesurfer_interface.input_files " \
                      "WHERE job_id = %s"
        cursor.execute(input_query, [workflow_id])
        for row in cursor.fetchall():
            input_files.append(row[0])
            input_files.append(os.path.dirname(row[0]))
    except psycopg2.Error as e:
        logger.exception("Error: {0}".format(e))
        return None
    finally:
        conn.close()
    return input_files


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
                "       jobs.username, " \
                "       jobs.state, " \
                "       job_run.pegasus_ts, " \
                "       jobs.subject " \
                "FROM freesurfer_interface.jobs AS jobs, " \
                "     freesurfer_interface.job_run AS job_run " \
                "WHERE state = 'DELETE PENDING' AND " \
                "      jobs.id = job_run.job_id"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'DELETED' " \
                 "WHERE id = %s;"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            workflow_id = row[0]
            username = row[1]
            logger.info("Deleting workflow {0} for user {1}".format(workflow_id,
                                                                    username))
            # pegasus_ts is stored as datetime in the database, convert it to what we have on the fs
            pegasus_ts = row[3]

            if pegasus_ts is None:
                # not submitted yet
                logger.info("Workflow {0} not ".format(workflow_id) +
                            "submitted, updating")
                cursor.execute(job_update, [workflow_id])
                if args.dry_run:
                    conn.rollback()
                else:
                    conn.commit()
                continue

            workflow_dir = os.path.join(fsurfer.FREESURFER_SCRATCH,
                                        username,
                                        'workflows',
                                        'fsurf',
                                        'pegasus',
                                        'freesurfer',
                                        pegasus_ts)
            result_dir = os.path.join(fsurfer.FREESURFER_BASE,
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
                else:
                    logger.error("Got error while removing workflow, "
                                 "exitcode: {0} error: {1}".format(exit_code, output))
            logger.info("Jobs removed, removing workflow directory\n")
            try:
                if not args.dry_run and os.path.exists(workflow_dir):
                    shutil.rmtree(workflow_dir)
            except shutil.Error:
                logger.exception("Can't remove directory at "
                                 "{0}, exiting...\n".format(workflow_dir))

            deletion_list = []
            # add input file
            input_files = get_input_files(workflow_id)
            if input_files is None:
                logger.error("Can't find input files for " +
                             "workflow {0}".format(workflow_id))
            else:
                deletion_list.extend(input_files)
            # remove files in result dir
            if os.path.isdir(result_dir):
                for entry in os.listdir(result_dir):
                    deletion_list.append(os.path.join(result_dir, entry))
            if os.path.exists(result_dir):
                deletion_list.append(result_dir)
            # delete output and log copied over after workflow completion
            # if present
            deletion_list.append(os.path.join(fsurfer.FREESURFER_BASE,
                                              username,
                                              'results',
                                              'recon_all-{0}.log'.format(workflow_id)))
            deletion_list.append(os.path.join(fsurfer.FREESURFER_BASE,
                                              username,
                                              'results',
                                              "{0}_{1}_output.tar.bz2".format(workflow_id,
                                                                              row[4])))
            for entry in deletion_list:
                if args.dry_run:
                    sys.stdout.write("Would delete {0}\n".format(entry))
                else:
                    logger.info("Removing {0}".format(entry))
                    if not purge_workflow_file(entry):
                        logger.error("Can't remove {0} for job {1}".format(entry,
                                                                           workflow_id))
            logger.info("Setting workflow {0} to DELETED".format(workflow_id))
            cursor.execute(job_update, [workflow_id])
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
