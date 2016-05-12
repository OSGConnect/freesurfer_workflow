#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license
import argparse
import subprocess
import sys
import log
from email.mime.text import MIMEText

import psycopg2

import helpers
import fsurfer


PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
VERSION = fsurfer.__version__


def email_user(workflow_id, email):
    """
    Email user informing them that a workflow will be deleted

    :param workflow_id: id for workflow that will be deleted
    :param email: email address for user
    :return: None
    """
    msg = MIMEText('The results from your freesurfer ' +
                   'workflow {0} '.format(workflow_id) +
                   'will be deleted in 7 days')

    msg['Subject'] = 'Results for freesurfer workflow {0} '.format(workflow_id)
    msg['Subject'] += 'will be deleted'
    sender = 'fsurf@login.osgconnect.net'
    dest = email
    msg['From'] = sender
    msg['To'] = dest
    try:
        sendmail = subprocess.Popen(['/usr/sbin/sendmail', '-t'], stdin=subprocess.PIPE)
        sendmail.communicate(msg.as_string())
    except subprocess.CalledProcessError:
        pass


def process_results():
    """
    Process results from jobs, removing any that are more than 30 days old

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

    conn = helpers.get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, username, email, state, pegasus_ts, subject " \
                "  FROM freesurfer_interface.jobs " \
                "WHERE (state = 'COMPLETED' OR" \
                "       state = 'ERROR') AND" \
                "      (age(job_date) >= '22 days' AND " \
                "      (age(job_date) < '23 days') ;"
    try:
        cursor.execute(job_query)
        for row in cursor.fetchall():
            logger.info("Warning user {0} about workflow {1} purge".format(row[0],
                                                                           row[1]))
            if args.dry_run:
                sys.stdout.write("Would email {0}".format(row[2]))
                sys.stdout.write("about workflow {0}\n".format(row[0]))
                continue
            if not email_user(row[0], row[2]):
                logger.error("Can't email {0} for job {1}".format(row[2],
                                                                  row[0]))
                continue
            conn.commit()
    except psycopg2.Error, e:
        logger.error("Got pgsql error: {0}".format(e))
        return 1
    finally:
        conn.commit()
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(process_results())
