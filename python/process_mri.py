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
import fsurfer.log

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
PEGASUSRC_PATH = '/stash2/user/fsurf/pegasusconf/pegasusrc'
VERSION = fsurfer.__version__


def pegasus_submit(dax, workflow_directory):
    """
    Submit a workflow to pegasus

    :param dax:  path to xml file with DAX, used for submit
    :param workflow_directory:  directory for pegasus to keep it's workflow
                              information
    :return:            the output from pegasus
    """
    try:
        output_directory = os.path.join(workflow_directory, 'output')
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


def submit_workflow(subject_files, version, subject_name, user, jobid,
                    multicore=True, options=None, workflow='diamond'):
    """
    Submit a workflow to OSG for processing

    :param subject_files:  lists with paths to files with MRI
                           data (mgz or subject dir)
    :param version:        FreeSurfer version to use
    :param subject_name:   name of subject being processed
    :param user:          freesurfer user that workflow is being run for
    :param jobid:         job id for the workflow
    :param multicore:     boolean indicating whether to use a multicore
                          workflow or not
    :param options:       Options to pass to FreeSurfer
    :param workflow:      string indicating type of workflow to run (serial,
                          diamond, single)
    :return:              0 on success, 1 on error
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
    workflow_directory = os.path.join(FREESURFER_BASE, user, 'workflows')
    if workflow == 'serial':
        created = fsurfer.create_serial_workflow(dax,
                                                 version,
                                                 cores,
                                                 dax_subject_files,
                                                 subject_name)
    elif workflow == 'diamond':
        created = fsurfer.create_diamond_workflow(dax,
                                                  version,
                                                  cores,
                                                  dax_subject_files,
                                                  subject_name)
    elif workflow == 'single':
        created = fsurfer.create_single_workflow(dax,
                                                 version,
                                                 cores,
                                                 dax_subject_files,
                                                 subject_name)
    elif workflow == 'custom':
        created = fsurfer.create_custom_workflow(dax,
                                                 version,
                                                 cores,
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
        dax.invoke('on_success', "/usr/bin/update_fsurf_job.py --success --id {0}".format(jobid))
        dax.invoke('on_error', "/usr/bin/update_fsurf_job.py --failure --id {0}".format(jobid))
        dax_name = "freesurfer_{0}.xml".format(curr_date)
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
        exit_code, output = pegasus_submit(dax="{0}".format(dax_name),
                                           workflow_directory=workflow_directory)
        logger.info("Submitted workflow, got exit code {0}".format(exit_code))
        logger.info("Pegasus output: {0}".format(output))
        if exit_code != 0:
            os.unlink(dax_name)
            return 1
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
                    job_update = "UPDATE freesurfer_interface.jobs  " \
                                 "SET pegasus_ts = %s" \
                                 "WHERE id = %s;"
                    try:
                        conn = get_db_client()
                        cursor = conn.cursor()
                        cursor.execute(job_update, [workflow_id, jobid])
                        conn.commit()
                        logger.info("Updated DB")
                        return 0
                    except psycopg2.Error as e:
                        logger.exception("Got pgsql error: {0}".format(e))
                        pass

                break

    return 1


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

    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, num_inputs, subject, options, version " \
                "FROM freesurfer_interface.jobs " \
                "WHERE state = 'QUEUED'"
    input_file_query = "SELECT filename, path, subject_dir " \
                       "FROM freesurfer_interface.input_files " \
                       "WHERE job_id = %s AND NOT purged"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'PROCESSING' " \
                 "WHERE id = %s;"
    job_error = "UPDATE freesurfer_interface.jobs " \
                "SET state = 'ERROR' " \
                "WHERE id = %s;"
    account_start = "INSERT INTO freesurfer_interface.job_run(job_id, " \
                    "                                         state, " \
                    "                                         tasks) " \
                    "VALUES(%s, 'QUEUED', %s)"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            logger.info("Processing workflow {0} for user {1}".format(row[0], row[1]))

            workflow_directory = os.path.join(FREESURFER_BASE, row[1])
            if not os.path.exists(workflow_directory):
                if args.dry_run:
                    sys.stdout.write("Would have created {0}".format(workflow_directory))
                else:
                    os.makedirs(workflow_directory)
            cursor2 = conn.cursor()
            cursor2.execute(input_file_query, [row[0]])
            input_files = []
            custom_workflow = False
            if cursor2.rowcount < 1:
                logger.error("No input files, skipping workflow {0}".format(row[0]))
                continue
            for input_info in cursor2.fetchall():
                input_file = os.path.join(FREESURFER_BASE, input_info[0], input_info[1])
                if not os.path.isfile(input_file):
                    logger.warn("Input file {0} missing, skipping".format(input_file))
                    break
                elif bool(input_info[2]):
                    # input file is a subject dir
                    custom_workflow = True
                    if cursor2.rowcount != 1:
                        # can't give multiple subject dirs at one time
                        logger.error("Subject dir combined with multiple inputs, skipping!")
                        cursor3 = conn.cursor()
                        if args.dry_run:
                            sys.stdout.write("Would have changed workflow "
                                             "{0} to ERROR state\n".format(row[0]))
                        else:
                            logger.error("Changed {0} to ERROR state".format(row[0]))
                            cursor3.execute(job_error, [row[0]])
                            break
                    input_files.append(input_file)
                else:
                    input_files.append(input_file)
            num_tasks = 0
            if not args.dry_run:
                if custom_workflow:
                    errors = submit_workflow(input_files, row[5], row[3],
                                             workflow_directory, row[0],
                                             options=row[4],
                                             workflow='custom')
                    num_tasks = 1
                else:
                    errors = submit_workflow(input_files, row[5], row[3],
                                             workflow_directory, row[0])
                    num_tasks = 4
            else:
                errors = False
            if not errors and not args.dry_run:
                cursor.execute(job_update, [row[0]])
                cursor.execute(account_start, [row[0], num_tasks])
                conn.commit()
                logger.info("Set workflow {0} status to PROCESSING".format(row[0]))
            else:
                conn.rollback()
                logger.info("Rolled back transaction")
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        pass

    fcntl.flock(x, fcntl.LOCK_UN)
    x.close()
    os.unlink('/tmp/fsurf_process.lock')

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
