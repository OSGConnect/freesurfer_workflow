#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
from email.mime.text import MIMEText

import psycopg2
import shutil
import fsurfer

VERSION = fsurfer.__version__

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'

EMAIL_TEMPLATE = '''
This email is being sent to inform you that your freesurfer workflow {0} submitted on {1}
has completed {2}.  You can download the output by running
`fsurf --output {0} --user {3} --password <pass>`
or download the Freesurfer log files by running `fsurf --log {0} --user {3} --password <pass>.`

Please contact support@osgconnect.net if you have any questions.
'''


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
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    info_query = "SELECT jobs.subject, " \
                 "       jobs.job_date, " \
                 "       date_trunc('second', jobs.pegasus_ts), " \
                 "       users.email, " \
                 "       users.username " \
                 "FROM freesurfer_interface.jobs AS jobs, " \
                 "     freesurfer_interface.users AS users " \
                 "WHERE jobs.id  = %s AND jobs.username = users.username"
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
    except psycopg2.Error, e:
        logger.error("Got pgsql error: {0}".format(e))
        return

    if success:
        status = 'succesfully'
    else:
        status = 'with errors'
    msg = MIMEText(EMAIL_TEMPLATE.format(jobid,
                                         submit_date,
                                         status,
                                         username))

    msg['Subject'] = 'Freesurfer workflow {0} completed'.format(jobid)
    sender = 'fsurf@login.osgconnect.net'
    msg['From'] = sender
    msg['To'] = user_email
    try:
        sendmail = subprocess.Popen(['/usr/sbin/sendmail', '-t'], stdin=subprocess.PIPE)
        sendmail.communicate(msg.as_string())
        logger.info("Emailing {0} about workflow {1}".format(user_email, jobid))
    except subprocess.CalledProcessError, e:
        logger.error("Can't email user, got exception: {0}".format(e))
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
    logger.info("Copied {0} to {1}".format(result_filename, output_filename))
    result_logfile = os.path.join(FREESURFER_BASE,
                                  username,
                                  'workflows',
                                  'output',
                                  'fsurf',
                                  'pegasus',
                                  'freesurfer',
                                  pegasus_ts,
                                  'recon-all.log')
    log_filename = os.path.join(FREESURFER_BASE,
                                username,
                                'results',
                                'recon_all-{0}.log'.format(jobid))
    shutil.copyfile(result_logfile, log_filename)
    logger.info("Copied {0} to {1}".format(result_logfile, log_filename))
    try:
        if success:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'COMPLETED'" \
                         "WHERE id = %s;"
            logger.info("Updating workflow {0} to COMPLETED".format(jobid))
        else:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'FAILED'" \
                         "WHERE id = %s;"
        logger.info("Updating workflow {0} to FAILED".format(jobid))
        cursor.execute(job_update, [jobid])
        conn.commit()
        conn.close()
    except psycopg2.Error, e:
        logger.error("Got pgsql error: {0}".format(e))
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
