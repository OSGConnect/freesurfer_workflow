#!/usr/bin/env python
import argparse
import getpass
import os
import subprocess
import sys
from email.mime.text import MIMEText

VERSION = '0.2'

EMAIL_TEMPLATE = '''
This email is being sent to inform you that your freesurfer workflow {0} has
completed {2}.  You can download the output by running `fsurf --output {0}`
or download the Freesurfer log files by running `fsurf --log {0} .`

Please contact support@osgconnect.net if you have any questions.
'''


def email_user(success=True):
    """
    Email user informing them that a workflow has completed

    :param success: True if workflow completed successfully
    :return: None
    """
    if 'PEGASUS_SUBMIT_DIR' in os.environ:
        workflow = os.path.basename(os.environ['PEGASUS_SUBMIT_DIR'])
    else:
        workflow = ''
    if success:
        status = 'succesfully'
    else:
        status = 'with errors'

    msg = MIMEText(EMAIL_TEMPLATE.format(workflow,
                                         status))
    msg['Subject'] = 'Freesurfer workflow {0} completed {1}'.format(workflow,
                                                                    status)
    sender = 'fsurf@login.osgconnect.net'
    dest = getpass.getuser()
    msg['From'] = sender
    msg['To'] = dest
    try:
        sendmail = subprocess.Popen(['/usr/sbin/sendmail', '-t'], stdin=subprocess.PIPE)
        sendmail.communicate(msg.as_string())
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
    # Arguments for workflow outcome
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
