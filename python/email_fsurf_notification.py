#!/usr/bin/env python
import argparse
import getpass
import subprocess
import sys
from email.mime.text import MIMEText

REST_ENDPOINT = "http://postgres.ci-connect.net/freesurfer"
VERSION = '0.1'


def email_user(success=True):
    """
    Email user informing them that a workflow has completed

    :param success: True if workflow completed successfully
    :return: None
    """
    if success:
        msg = MIMEText('Your freesurfer workflow has completed succesfully')
    else:
        msg = MIMEText('Your freesurfer workflow has completed with errors')
    msg['Subject'] = 'Freesurfer workflow completed'
    sender = 'fsurf@login.osgconnect.net'
    dest = getpass.getuser()
    msg['From'] = sender
    msg['To'] = dest
    try:
        sendmail = subprocess.check_call(['/usr/bin/sendmail -t'], stdin=subprocess.PIPE)
        sendmail.stdin(msg.as_string())
    except subprocess.CalledProcessError:
        pass


def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Process freesurfer information")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments for action
    parser.add_argument('--success', dest='success',
                        action='store_true',
                        help='Workflow completed successfully')
    parser.add_argument('--failure', dest='success',
                        action='store_false',
                        help='Workflow completed with errors')

    args = parser.parse_args(sys.argv[1:])
    if args.success:
        email_user(success=True)
    else:
        email_user(success=False)

    sys.exit(0)

if __name__ == '__main__':
    main()
