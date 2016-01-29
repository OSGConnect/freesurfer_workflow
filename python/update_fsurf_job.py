#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
import tempfile
from email.mime.text import MIMEText

import psycopg2
import shutil

VERSION = '0.1'

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'


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


def process_results(jobid, success=True):
    """
    Email user informing them that a workflow has completed

    :param success: True if workflow completed successfully
    :param jobid: id for workflow
    :return: None
    """
    info_query = "SELECT jobs.subject, " \
                 "       jobs.job_date, " \
                 "       jobs.pegasus_ts, " \
                 "       users.email, " \
                 "       users.username " \
                 "FROM freesurfer_interface.jobs AS jobs, " \
                 "     freesurfer_interface.users AS users " \
                 "WHERE jobs.id  = %s"
    conn = get_db_client()
    cursor = conn.cursor()
    try:
        cursor.execute(info_query, [jobid])
        row = cursor.fetchone()
        if row:
            subject_name = row[0]
            submit_date = row[1]
            pegasus_ts = row[2]
            user_email = row[3]
            username = row[4]
        else:
            return
    except psycopg2.Error:
        return
    if success:
        msg = MIMEText('Your freesurfer workflow {0}'.format(jobid) +
                       'submitted on {0} and processing '.format(submit_date) +
                       'subject {0} '.format(subject_name) +
                       'has completed succesfully')
    else:
        msg = MIMEText('Your freesurfer workflow {0}'.format(jobid) +
                       'submitted on {0} and processing '.format(submit_date) +
                       'subject {0} '.format(subject_name) +
                       'has completed with errors')

    msg['Subject'] = 'Freesurfer workflow {0} completed'.format(jobid)
    sender = 'fsurf@login.osgconnect.net'
    msg['From'] = sender
    msg['To'] = user_email
    try:
        sendmail = subprocess.Popen(['/usr/sbin/sendmail', '-t'], stdin=subprocess.PIPE)
        sendmail.communicate(msg.as_string())
    except subprocess.CalledProcessError:
        pass

    # copy output to the results directory
    output_filename = os.path.join(FREESURFER_BASE,
                                   username,
                                   'results',
                                   "{0}_{1}_output.tar.bz2".format(jobid,
                                                                   subject_name))
    result_filename = os.path.join(FREESURFER_BASE,
                                   username,
                                   'workflows',
                                   'output',
                                   'fsurf',
                                   'pegasus',
                                   'freesurfer',
                                   pegasus_ts,
                                   '{0}_output.tar.bz2'.format(subject_name))
    shutil.copyfile(result_filename, output_filename)
    result_logfile = os.path.join(FREESURFER_BASE,
                                  username,
                                  'workflows',
                                  'pegasus',
                                  'freesurfer',
                                  pegasus_ts,
                                  'recon-all.log')
    log_filename = os.path.join(FREESURFER_BASE,
                                username,
                                'results',
                                'recon_all-{0}.log'.format(jobid))
    shutil.copyfile(result_logfile, log_filename)
    try:
        if success:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'COMPLETED'" \
                         "WHERE id = %s;"
        else:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'FAILED'" \
                         "WHERE id = %s;"
        cursor.execute(job_update, jobid)
        conn.commit()
        conn.close()
    except psycopg2.Error:
        return



def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Process freesurfer information")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments for workflow outcome
    parser.add_argument('--success', dest='success',
                        action='store_true',
                        help='Workflow completed successfully')
    parser.add_argument('--failure', dest='success',
                        action='store_false',
                        help='Workflow completed with errors')
    # Arguments identifying workflow
    parser.add_argument('--id', dest='workflow_id',
                        action='store', help='Pegasus workflow id to use')

    args = parser.parse_args(sys.argv[1:])
    if args.success:
        process_results(args.workflow_id, success=True)
    else:
        process_results(args.workflow_id, success=False)

    sys.exit(0)

if __name__ == '__main__':
    main()
