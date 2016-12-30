#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license

# Process jobs that have been uploaded and create
# a pegasus workflow and submit
import argparse
import re
import sys
import os
import time
import subprocess
import fcntl

import cStringIO
import psycopg2

import Pegasus.DAX3
import fsurfer
import fsurfer.helpers
import fsurfer.log

PEGASUSRC_PATH = '/etc/fsurf/pegasusconf/pegasusrc'
VERSION = fsurfer.__version__
MAX_RUNNING_WORKFLOWS = 200


def pegasus_submit(dax, workflow_directory, output_directory):
    """
    Submit a workflow to pegasus

    :param dax:  path to xml file with DAX, used for submit
    :param workflow_directory:  directory for workflow information
    :param output_directory:  directory for workflow output
    :return:            the output from pegasus
    """
    try:
        output = subprocess.check_output(['/usr/bin/pegasus-plan',
                                          '--sites',
                                          'condorpool',
                                          '--dir',
                                          workflow_directory,
                                          '--conf',
                                          PEGASUSRC_PATH,
                                          '--output-dir',
                                          output_directory,
                                          '--dax',
                                          dax,
                                          '--submit'],
                                         stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as err:
        return err.returncode, err.output

    return 0, output


def submit_workflow(subject_files, version, subject_name, user, job_run_id,
                    multicore=True, options=None, workflow='diamond'):
    """
    Submit a workflow to OSG for processing

    :param subject_files:  lists with paths to files with MRI
                           data (mgz or subject dir)
    :param version:        FreeSurfer version to use
    :param subject_name:   name of subject being processed
    :param user:          freesurfer user that workflow is being run for
    :param job_run_id:         job run id for the workflow
    :param multicore:     boolean indicating whether to use a multicore
                          workflow or not
    :param options:       Options to pass to FreeSurfer
    :param workflow:      string indicating type of workflow to run (serial,
                          diamond, single)
    :return:              pegasus workflow id  on success, None on error
    """
    if multicore:
        cores = 8
    else:
        cores = 2
    logger = fsurfer.log.get_logger()

    logger.debug("Processing workflow using {0} as input".format(subject_files))
    dax = Pegasus.DAX3.ADAG('freesurfer')
    dax_subject_files = []
    for input_file in subject_files:
        dax_subject_file = Pegasus.DAX3.File(os.path.basename(input_file))
        dax_subject_file.addPFN(Pegasus.DAX3.PFN("file://{0}".format(input_file),
                                                 "local"))
        dax_subject_files.append(dax_subject_file)
        dax.addFile(dax_subject_file)
    workflow_directory = os.path.join(fsurfer.FREESURFER_SCRATCH, user, 'workflows')
    output_directory = os.path.join(fsurfer.FREESURFER_BASE, user, 'workflows', 'output')
    job_invoke_cmd = "/usr/bin/task_completed.py --id {0}".format(job_run_id)
    if workflow == 'serial':
        created = fsurfer.create_serial_workflow(dax,
                                                 version,
                                                 cores,
                                                 dax_subject_files,
                                                 subject_name,
                                                 invoke_cmd=job_invoke_cmd)
    elif workflow == 'diamond':
        created = fsurfer.create_diamond_workflow(dax,
                                                  version,
                                                  cores,
                                                  dax_subject_files,
                                                  subject_name,
                                                  invoke_cmd=job_invoke_cmd)
    elif workflow == 'single':
        created = fsurfer.create_single_workflow(dax,
                                                 version,
                                                 cores,
                                                 dax_subject_files,
                                                 subject_name)
    elif workflow == 'custom':
        created = fsurfer.create_custom_workflow(dax,
                                                 version,
                                                 2,  # custom workflows get 2 cores
                                                 dax_subject_files[0],
                                                 subject_name,
                                                 options)
    else:
        created = fsurfer.create_serial_workflow(dax,
                                                 version,
                                                 cores,
                                                 dax_subject_files,
                                                 subject_name)
    if created:
        curr_date = time.strftime("%Y%m%d_%H%M%S", time.gmtime(time.time()))
        dax.invoke('on_success', "/usr/bin/workflow_completed.py --success --id {0}".format(job_run_id))
        dax.invoke('on_error', "/usr/bin/workflow_completed.py --failure --id {0}".format(job_run_id))
        dax_name = "freesurfer_{0}.xml".format(curr_date)
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
        exit_code, output = pegasus_submit(dax="{0}".format(dax_name),
                                           workflow_directory=workflow_directory,
                                           output_directory=output_directory)
        logger.info("Submitted workflow, got exit code {0}".format(exit_code))
        logger.info("Pegasus output: {0}".format(output))
        if exit_code != 0:
            os.unlink(dax_name)
            return None
        os.unlink(dax_name)
        capture_id = False
        for line in cStringIO.StringIO(output).readlines():
            if 'Your workflow has been started' in line:
                capture_id = True
            if capture_id and workflow_directory in line:
                id_match = re.search(r'([T\d]+-\d+)'.format(workflow_directory),
                                     line)
                if id_match is not None:
                    workflow_id = id_match.group(1)
                    return workflow_id

    return None


def exceeded_running_limit(conn):
    """
    Check to see if the number of workflows in
    RUNNING state is more than MAX_RUNNING_WORKFLOWS

    :param conn: database connection to use
    :return: True if condition holds, False otherwise
    """
    running_workflow_query = "SELECT COUNT(*) " \
                             "FROM freesurfer_interface.jobs " \
                             "WHERE state = 'RUNNING'"
    try:
        fsurfer.log.initialize_logging()
        logger = fsurfer.log.get_logger()
        cursor = conn.cursor()
        cursor.execute(running_workflow_query)
        running_workflows = cursor.fetchone()[0]
        if running_workflows >= MAX_RUNNING_WORKFLOWS:
            logger.warn("Number of running workflows at or above" +
                        "MAX_RUNNING_WORKFLOWS: " +
                        "{0} vs {1}".format(running_workflows,
                                            MAX_RUNNING_WORKFLOWS))
            return True
        return False
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return False


def process_images():
    """
    Process uploaded images

    :return: exit code (0 for success, non-zero for failure)
    """
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
    try:
        x = open('/tmp/fsurf_process.lock', 'w+')
        fcntl.flock(x, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logger.warn('Lock file present, exiting')
        sys.exit(1)

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, num_inputs, subject, options, version " \
                "FROM freesurfer_interface.jobs " \
                "WHERE state = 'QUEUED'"
    input_file_query = "SELECT filename, path, subject_dir " \
                       "FROM freesurfer_interface.input_files " \
                       "WHERE job_id = %s AND NOT purged"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'RUNNING' " \
                 "WHERE id = %s;"
    job_run_update = "UPDATE freesurfer_interface.job_run " \
                     "SET pegasus_ts = %s " \
                     "WHERE id = %s;"
    job_error = "UPDATE freesurfer_interface.jobs " \
                "SET state = 'ERROR' " \
                "WHERE id = %s;"

    account_start = "INSERT INTO freesurfer_interface.job_run(job_id, " \
                    "                                         tasks) " \
                    "VALUES(%s, %s) " \
                    "RETURNING id"
    try:
        cursor.execute(job_query)
        if exceeded_running_limit(conn):
            logger.warn("Max number of running workflows reached, exiting")
            fcntl.flock(x, fcntl.LOCK_UN)
            x.close()
            os.unlink('/tmp/fsurf_process.lock')
            return 0
        for row in cursor.fetchall():
            workflow_id = row[0]
            username = row[1]
            logger.info("Processing workflow {0} for user {1}".format(workflow_id,
                                                                      username))

            workflow_directory = os.path.join('/local-scratch',
                                              'fsurf',
                                              username,
                                              'workflows')
            if not os.path.exists(workflow_directory):
                if args.dry_run:
                    sys.stdout.write("Would have created {0}".format(workflow_directory))
                else:
                    os.makedirs(workflow_directory)
            cursor2 = conn.cursor()
            cursor2.execute(input_file_query, [workflow_id])
            input_files = []
            custom_workflow = False
            if cursor2.rowcount < 1:
                logger.error("No input files, skipping workflow {0}".format(workflow_id))
                continue
            for input_info in cursor2.fetchall():
                input_file = os.path.join(fsurfer.FREESURFER_BASE,
                                          input_info[0],
                                          input_info[1])
                if not os.path.isfile(input_file):
                    logger.warn("Input file {0} missing, skipping".format(input_file))
                    continue
                elif bool(input_info[2]):
                    # input file is a subject dir
                    custom_workflow = True
                    if cursor2.rowcount != 1:
                        # can't give multiple subject dirs at one time
                        logger.error("Subject dir combined with multiple inputs, skipping!")
                        cursor3 = conn.cursor()
                        if args.dry_run:
                            sys.stdout.write("Would have changed workflow "
                                             "{0} to ERROR state\n".format(workflow_id))
                        else:
                            logger.error("Changed {0} to ERROR state".format(workflow_id))
                            cursor3.execute(job_error, [workflow_id])
                            continue
                    input_files.append(input_file)
                else:
                    input_files.append(input_file)

            pegasus_ts = None
            if custom_workflow and not args.dry_run:
                cursor.execute(account_start, [workflow_id, 1])
                job_run_id = cursor.fetchone()[0]
                pegasus_ts = submit_workflow(input_files,
                                             version=row[5],
                                             subject_name=row[3],
                                             user=username,
                                             job_run_id=job_run_id,
                                             options=row[4],
                                             workflow='custom')
            elif not args.dry_run:
                cursor.execute(account_start, [workflow_id, 4])
                job_run_id = cursor.fetchone()[0]
                pegasus_ts = submit_workflow(input_files,
                                             version=row[5],
                                             subject_name=row[3],
                                             user=username,
                                             job_run_id=job_run_id)
            if pegasus_ts:
                cursor.execute(job_run_update, [pegasus_ts,
                                                job_run_id])
            if pegasus_ts and not args.dry_run:
                cursor.execute(job_update, [workflow_id])
                conn.commit()
                logger.info("Set workflow {0} status to RUNNING".format(workflow_id))
            else:
                conn.rollback()
                logger.info("Rolled back transaction")
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        pass
    finally:
        conn.close()

    fcntl.flock(x, fcntl.LOCK_UN)
    x.close()
    os.unlink('/tmp/fsurf_process.lock')
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
    sys.exit(process_images())
