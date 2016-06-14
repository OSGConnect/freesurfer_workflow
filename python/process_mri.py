#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license

# Process jobs that have been uploaded and create
# a pegasus workflow and submit

import re
import sys
import os
import time
import subprocess

import cStringIO
import psycopg2

import Pegasus.DAX3
import fsurfer
import fsurfer.log

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
PEGASUSRC_PATH = '/stash2/user/fsurf/pegasusconf/pegasusrc'


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


def submit_workflow(subject_file, user, jobid, multicore=True, workflow='diamond'):
    """
    Submit a workflow to OSG for processing

    :param subject_file:  path to file with MRI data in mgz format
    :param user:          freesurfer user that workflow is being run for
    :param jobid:         job id for the workflow
    :param multicore:     boolean indicating whether to use a multicore
                          workflow or not
    :param workflow:      string indicating type of workflow to run (serial,
                          diamond, single)
    :return:              0 on success, 1 on error
    """
    if multicore:
        cores = 8
    else:
        cores = 2
    logger = fsurfer.log.get_logger()
    subject_name = os.path.basename(subject_file).replace("_defaced.mgz", "")
    logger.debug("Processing workflow using {0} as input".format(subject_file))
    dax = Pegasus.DAX3.ADAG('freesurfer')
    dax_subject_file = Pegasus.DAX3.File("{0}_defaced.mgz".format(subject_name))
    dax_subject_file.addPFN(Pegasus.DAX3.PFN("file://{0}".format(subject_file),
                                             "local"))
    dax.addFile(dax_subject_file)
    workflow_directory = os.path.join(FREESURFER_BASE, user, 'workflows')
    if workflow == 'serial':
        errors = fsurfer.create_serial_workflow(dax,
                                                cores,
                                                dax_subject_file,
                                                subject_name)
    elif workflow == 'diamond':
        errors = fsurfer.create_diamond_workflow(dax,
                                                 cores,
                                                 dax_subject_file,
                                                 subject_name)
    elif workflow == 'single':
        errors = fsurfer.create_single_workflow(dax,
                                                cores,
                                                dax_subject_file,
                                                subject_name)
    else:
        errors = fsurfer.create_serial_workflow(dax,
                                                cores,
                                                dax_subject_file,
                                                subject_name)
    if not errors:
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
                    except psycopg2.Error as e:
                        logger.exception("Got pgsql error: {0}".format(e))
                        pass

                break

    return errors


def process_images():
    """
    Process uploaded images

    :return: exit code (0 for success, non-zero for failure)
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, image_filename FROM freesurfer_interface.jobs " \
                "WHERE state = 'UPLOADED'"
    job_update = "UPDATE freesurfer_interface.jobs " \
                 "SET state = 'PROCESSING' " \
                 "WHERE id = %s;"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            logger.info("Processing workflow {0} for user {1}".format(row[0], row[1]))
            input_file = os.path.join(FREESURFER_BASE, row[1], 'input', row[2])
            workflow_directory = os.path.join(FREESURFER_BASE, row[1])
            if not os.path.exists(workflow_directory):
                os.makedirs(workflow_directory)
            if not os.path.isfile(input_file):
                continue
            errors = submit_workflow(input_file, workflow_directory, row[0])
            if not errors:
                cursor.execute(job_update, [row[0]])
                conn.commit()
                logger.info("Set workflow {0} status to PROCESSING".format(row[0]))
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        pass


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
