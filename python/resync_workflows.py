#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license

# Resync workflows with pegasus state if possible
import argparse
import sys
import os
import subprocess

import cStringIO
import psycopg2

import fsurfer
import fsurfer.log

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
PEGASUSRC_PATH = '/etc/fsurf/pegasusconf/pegasusrc'
VERSION = fsurfer.__version__
DRY_RUN = False


def reset_workflow(workflow_id):
    """
    Reset workflow to QUEUED state if possible so that it can be retried

    :param workflow_id: workflow id
    :return: exit code for sys.exit
    """

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    if DRY_RUN:
        sys.stdout.write("Resetting workflow {0}\n".format(workflow_id))
    job_run_delete = "DELETE FROM freesurfer_interface.job_run " \
                     "WHERE job_id = %s"
    job_reset = "UPDATE freesurfer_interface.jobs " \
                "SET state = 'QUEUED' " \
                "WHERE id = %s "
    try:
        cursor.execute(job_run_delete, [workflow_id])
        cursor.execute(job_reset, [workflow_id])
        if DRY_RUN:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()


def resync_workflows():
    """
    Check all workflows running for more than a day and see if they
    need to be resynced

    :return: exit code
    """
    global DRY_RUN
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()
    parser = argparse.ArgumentParser(description="Generate and submit "
                                                 "workflows to process jobs")
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
        DRY_RUN = True

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT jobs.id, jobs.username, job_run.pegasus_ts " \
                "FROM freesurfer_interface.jobs AS jobs," \
                "     freesurfer_interface.job_run AS job_run " \
                "WHERE state = 'RUNNING' AND " \
                "      age(job_date) >= '1 day' AND " \
                "      jobs.id = job_run.job_id"
    update_workflow_state = "UPDATE freesurfer_interface.jobs " \
                            "SET state = %s " \
                            "WHERE id = %s "

    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            workflow_id = row[0]
            username = row[1]
            logger.info("Examining workflow {0} started ".format(workflow_id) +
                        "by user {1}".format(username))
            if row[2] is None:
                logger.error("No workflow directory for running "
                             "workflow {0}".format(workflow_id))
                return reset_workflow(workflow_id)
            scratch_directory = os.path.join(fsurfer.FREESURFER_SCRATCH,
                                             username,
                                             'workflows',
                                             'fsurf',
                                             'pegasus',
                                             'freesurfer',
                                             row[2])
            try:
                output = subprocess.check_output(['/usr/bin/pegasus-status',
                                                  '-l',
                                                  scratch_directory],
                                                 stderr=subprocess.STDOUT)
                if 'Failure' in cStringIO.StringIO(output).readlines()[2]:
                    if DRY_RUN:
                        sys.stdout.write("Would have failed "
                                         "workflow {0}\n".format(workflow_id))
                        continue
                    cursor.execute(update_workflow_state, ['FAILED',
                                                           workflow_id])
                    subprocess.check_call(['/usr/bin/workflow_completed.py',
                                           '--failure',
                                           '--id',
                                           str(workflow_id)])
                elif 'Success' in cStringIO.StringIO(output).readlines()[2]:
                    if DRY_RUN:
                        sys.stdout.write("Would have completed "
                                         "workflow {0}\n".format(workflow_id))
                        continue
                    cursor.execute(update_workflow_state, ['COMPLETED',
                                                           workflow_id])
                    subprocess.check_call(['/usr/bin/workflow_completed.py',
                                           '--success',
                                           '--id',
                                           str(workflow_id)])
            except subprocess.CalledProcessError as err:
                conn.rollback()
                logger.error("Couldn't run commands: {0}".format(err))
            else:
                conn.rollback()
                logger.error("Rolled back transaction")
        if DRY_RUN:
            conn.rollback()
        else:
            conn.commit()
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        pass
    finally:
        conn.close()


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
    sys.exit(resync_workflows())
