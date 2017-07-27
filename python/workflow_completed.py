#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
import shutil
import xml
import xml.parsers.expat
import xml.dom.minidom
import re
from email.mime.text import MIMEText

import datetime
import dateutil.parser
import psycopg2

import fsurfer
import fsurfer.helpers

VERSION = fsurfer.__version__

SUCCESS_EMAIL_TEMPLATE = '''
This email is being sent to inform you that your FreeSurfer workflow {0}
submitted on {1} has completed successfully.  You can download the
output by running `fsurf output --id {0} ` or download the FreeSurfer
log files by running `fsurf output --id {0} --log-only .`

{2}

Please contact user-support@opensciencegrid.org  if you have any questions.
'''

FAIL_EMAIL_TEMPLATE = '''
This email is being sent to inform you that your FreeSurfer workflow {0}
submitted on {1} has been removed or has completed with errors.  You may
be able download the output by running `fsurf output --id {0} ` or download
the FreeSurfer log files by running `fsurf output --id {0} --log-only .`

Please note the output from FreeSurfer or the log files may not be
available depending on the type of error that was encountered.

{2}

Please contact user-support@opensciencegrid.org  if you have any questions.
'''

# helper class for time delta calculations
ZERO = datetime.timedelta(0)


class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


def format_seconds(duration, max_comp=2):
    """
    Utility for converting time to a readable format

    :param duration: time in seconds and miliseconds
    :param max_comp: number of components of the returned time
    :return: time in n component format
    """
    comp = 0
    if duration is None:
        return '-'
    # milliseconds = math.modf(duration)[0]
    sec = int(duration)
    formatted_duration = ''
    years = sec / 31536000
    sec -= 31536000 * years
    days = sec / 86400
    sec -= 86400 * days
    hrs = sec / 3600
    sec -= 3600 * hrs
    mins = sec / 60
    sec -= 60 * mins

    # years
    if comp < max_comp and (years >= 1 or comp > 0):
        comp += 1
        if days == 1:
            formatted_duration += str(years) + ' year, '
        else:
            formatted_duration += str(years) + ' years, '

    # days
    if comp < max_comp and (days >= 1 or comp > 0):
        comp += 1
        if days == 1:
            formatted_duration += str(days) + ' day, '
        else:
            formatted_duration += str(days) + ' days, '

    # hours
    if comp < max_comp and (hrs >= 1 or comp > 0):
        comp += 1
        if hrs == 1:
            formatted_duration += str(hrs) + ' hr, '
        else:
            formatted_duration += str(hrs) + ' hrs, '

    # mins
    if comp < max_comp and (mins >= 1 or comp > 0):
        comp += 1
        if mins == 1:
            formatted_duration += str(mins) + ' min, '
        else:
            formatted_duration += str(mins) + ' mins, '

    # seconds
    if comp < max_comp and (sec >= 1 or comp > 0):
        comp += 1
        if sec == 1:
            formatted_duration += str(sec) + " sec, "
        else:
            formatted_duration += str(sec) + " secs, "

    if formatted_duration[-2:] == ", ":
        formatted_duration = formatted_duration[:-2]

    return formatted_duration


def get_result_base_dir(workflow_info):
    """
    Return the base directory for results

    :param workflow_info: dictionary with workflow info
    :return: a string with the directory path to the results
    """
    return os.path.join(fsurfer.FREESURFER_BASE,
                        workflow_info['username'],
                        'workflows',
                        'output',
                        'fsurf',
                        'pegasus',
                        'freesurfer',
                        workflow_info['pegasus_ts'])


def parse_ks_record(fname):
    """
    Parses a Pegasus kickstart XML records

    :param fname: the filename of the job output file
    :return: a data dicttionary with a small set of keys/values from the record
    """

    data = {'duration': 0.0, 'utime': 0.0}

    dom_tree = xml.dom.minidom.parse(fname)
    collection = dom_tree.documentElement

    for mainjob in collection.getElementsByTagName('mainjob'):
        data['start'] = mainjob.getAttribute('start')
        data['duration'] += float(mainjob.getAttribute("duration"))
        for usage in mainjob.getElementsByTagName('usage'):
            data['utime'] += float(usage.getAttribute('utime'))

    # convert start date to unix ts
    # example: 2016-05-18T12:55:23.507-05:00
    dt = dateutil.parser.parse(data['start'])
    # epoch calculations in Python 2.6 makes me sad
    td = dt - datetime.datetime(1970, 1, 1, tzinfo=UTC())
    data['start_ts'] = (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6 

    data['end_ts'] = data['start_ts'] + int(data['duration'])

    return data


def parse_submit_file(fname):
    """
    Retrieves the HTCondor classad from a submit file

    :param fname: full path to the submit file
    :return: a dictionary of the classad
    """

    ad = {'request_cpus': 1}
    with open(fname) as f:
        for line in f:
            line = line.strip()
            if line == '' or line[0] == '#':
                continue
            if line.find('=') == -1:
                continue
            key, value = line.split('=', 1)
            key = key.strip().lower()
            value = value.strip()
            ad[key] = value
    return ad


def calculate_usage(submit_dir):
    """
    walks a Pegasus workflow directory and calculates the walltime and cpu usage

    :param submit_dir: the Pegasus workflow submit dir
    :return: a tuple with [walltime, cputime] used in seconds
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    start_ts = float('inf')
    end_ts = -float('inf')
    total_core_time = 0.0
    if not os.path.isdir(submit_dir):
        return [None, None]
    for f in os.listdir(submit_dir):

        # only consider files with out and numbers in them
        if not re.search('\.out\.[0-9]+$', f):
            continue

        full_name = os.path.join(submit_dir, f)

        # parse the kickstart record, if available
        try:
            ks = parse_ks_record(full_name)
            submit_file = re.sub('\.out\.[0-9]+$', '.sub', full_name)
            submit = parse_submit_file(submit_file)
        except xml.parsers.expat.ExpatError as e:
            # not a valid xml file
            # expect this since some of the files we
            # look at are not XML
            logger.info("Got exception while parsing {1}:\n{0}".format(full_name,
                                                                       e))
            continue

        cores = int(submit['request_cpus'])
        total_core_time += cores * ks['duration']

        if ks['start_ts'] < start_ts:
            start_ts = ks['start_ts']
        if ks['end_ts'] > end_ts:
            end_ts = ks['end_ts']

    # only return data if it is valid
    if end_ts - start_ts > 0 and total_core_time > 0:
        return [end_ts - start_ts,
                total_core_time]

    return [None, None]


def usage_msg(walltime, cputime):
    """
    Return a message summarizing usage

    :param walltime: walltime used by workflow
    :param cputime:  cputime used by workflow
    :return: a string summarizing the usage
    """

    msg = ""
    if walltime and cputime:
        # only return message if it is valid
        msg = '\nThe workflow was active for ' + format_seconds(walltime) \
            + ' and used a total CPU time of ' \
            + format_seconds(cputime) + ' on the Open Science Grid.' \
            + ' Please note that the CPU time' \
            + ' might be larger than the active time due to multi-threading.\n'

    return msg


def recover_logs(pegasus_workflow_id):
    """
    Try to recover any logs from a pegasus workflow that has failed. Recovered
    logs copied to output directory


    :param pegasus_workflow_id: workfow id to check
    :return: None
    """
    pass


def get_workflow_info(conn, job_run_id):
    """
    Get workflow information for a specified workflow
    
    :param conn: active pgsql connection to use
    :param job_run_id: id for a workflow's job run to query
    :return: a dict with (subject_name, submit_date, pegasus_ts, 
                          user_email, username, job_id)
    :raises psycopg2.Error
    """
    logger = fsurfer.log.get_logger()
    info_query = "SELECT jobs.subject, " \
                 "       date_trunc('second'," \
                 "                  jobs.job_date), " \
                 "       date_trunc('second', " \
                 "                  job_run.pegasus_ts::timestamp with time zone), " \
                 "       users.email, " \
                 "       users.username," \
                 "       jobs.id " \
                 "FROM freesurfer_interface.jobs AS jobs, " \
                 "     freesurfer_interface.job_run AS job_run, " \
                 "     freesurfer_interface.users AS users " \
                 "WHERE job_run.id  = %s AND " \
                 "      jobs.username = users.username AND " \
                 "      jobs.id = job_run.job_id"
    cursor = conn.cursor()
    cursor.execute(info_query, [job_run_id])
    row = cursor.fetchone()
    results = {}
    if row:
        results['subject_name'] = row[0]
        results['submit_date'] = row[1]
        pegasus_ts = row[2]
        # pegasus_ts is stored as datetime in the database, convert it to what we have on the fs
        results['pegasus_ts'] = pegasus_ts.strftime('%Y%m%dT%H%M%S%z')
        results['user_email'] = row[3]
        results['username'] = row[4]
        results['job_id'] = row[5]
        return results
    else:
        logger.error("No matches to query: "
                     "{0}".format(cursor.mogrify(info_query, [job_run_id])))
        return


def email_user(workflow_info, success, stats_text):
    """
    Email user with a message indicating that their job has completed.
    
    :param workflow_info: dictionary with information about user workflow
    :param success: Boolean indicating whether workflow has succeeded or not
    :param stats_text: string with message about workflow statistics
    :return: None 
    """
    logger = fsurfer.log.get_logger()

    if success:
        msg = MIMEText(SUCCESS_EMAIL_TEMPLATE.format(workflow_info['job_id'],
                                                     workflow_info['submit_date'],
                                                     stats_text))

    else:
        msg = MIMEText(FAIL_EMAIL_TEMPLATE.format(workflow_info['job_id'],
                                                  workflow_info['submit_date'],
                                                  stats_text))
    msg['Subject'] = 'FreeSurfer workflow {0} completed'.format(workflow_info['job_id'])
    sender = 'fsurf@login.osgconnect.net'
    msg['From'] = sender
    msg['To'] = workflow_info['user_email']
    try:
        sendmail = subprocess.Popen(['/usr/sbin/sendmail', '-t'],
                                    stdin=subprocess.PIPE)
        sendmail.communicate(msg.as_string())
        logger.info("Emailing {0} about workflow {1}".format(workflow_info['user_email'],
                                                             workflow_info['job_id']))
    except subprocess.CalledProcessError as e:
        # don't want exceptions when emailing to stop rest of processing
        logger.exception("Can't email user, got exception: {0}".format(e))
        pass


def copy_outputs(workflow_info, success):
    """
    Copy outputs from a workflow run to the appropriate locations
    
    :param workflow_info: dictionary with information about user workflow
    :param success: Boolean indicating whether workflow has succeeded or not
    :return: None 
    """
    logger = fsurfer.log.get_logger()
    # copy output to the results directory
    output_filename = os.path.join(fsurfer.FREESURFER_BASE,
                                   workflow_info['username'],
                                   'results',
                                   "{0}_{1}_output.tar.bz2".format(workflow_info['job_id'],
                                                                   workflow_info['subject_name']))
    result_filename = os.path.join(get_result_base_dir(workflow_info),
                                   '{0}_output.tar.bz2'.format(workflow_info['subject_name']))
    try:
        if not os.path.isfile(result_filename):
            logger.error("Output file {0} not found".format(result_filename))
        else:
            shutil.copyfile(result_filename, output_filename)
    except shutil.Error as e:
        logger.exception("Exception while copying file: {0}".format(e))
    except IOError as e:
        logger.exception("Exception while copying file: {0}".format(e))
    logger.info("Copied {0} to {1}".format(result_filename, output_filename))
    result_logfile = os.path.join(get_result_base_dir(workflow_info),
                                  'recon-all.log')
    log_filename = os.path.join(fsurfer.FREESURFER_BASE,
                                workflow_info['username'],
                                'results',
                                'recon_all-{0}.log'.format(workflow_info['job_id']))
    try:
        if not os.path.isfile(result_logfile):
            logger.error("Output file {0} not found".format(result_logfile))
            if not success:
                recover_logs(workflow_info['pegasus_ts'])
        else:
            shutil.copyfile(result_logfile, log_filename)
    except shutil.Error as e:
        logger.exception("Exception while copying file: {0}".format(e))
    except IOError as e:
        logger.exception("Exception while copying file: {0}".format(e))
    logger.info("Copied {0} to {1}".format(result_logfile, log_filename))


def process_results(job_run_id, success=True):
    """
    Email user informing them that a workflow has completed

    :param success: True if workflow completed successfully
    :param job_run_id: id for job run id for workflow
    :return: None
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    conn = fsurfer.helpers.get_db_client()
    cursor = conn.cursor()
    try:
        workflow_info = get_workflow_info(conn, job_run_id)
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return

    stats_text = ""
    walltime = 0
    cputime = 0
    try:
        submit_dir = os.path.join(fsurfer.FREESURFER_SCRATCH,
                                  workflow_info['username'],
                                  'workflows',
                                  'fsurf',
                                  'pegasus',
                                  'freesurfer',
                                  workflow_info['pegasus_ts'])
        walltime, cputime = calculate_usage(submit_dir)
        stats_text = usage_msg(walltime, cputime)
    except Exception as e:
        logger.exception("Can't calculate stats, got exception: {0}".format(e))
        pass

    email_user(workflow_info, success, stats_text)

    # copy output to the results directory
    output_filename = os.path.join(fsurfer.FREESURFER_BASE,
                                   workflow_info['username'],
                                   'results',
                                   "{0}_{1}_output.tar.bz2".format(workflow_info['job_id'],
                                                                   workflow_info['subject_name']))
    result_filename = os.path.join(get_result_base_dir(workflow_info),
                                   '{0}_output.tar.bz2'.format(workflow_info['subject_name']))
    try:
        if not os.path.isfile(result_filename):
            logger.error("Output file {0} not found".format(result_filename))
        else:
            shutil.copyfile(result_filename, output_filename)
    except shutil.Error as e:
        logger.exception("Exception while copying file: {0}".format(e))
    except IOError as e:
        logger.exception("Exception while copying file: {0}".format(e))
    logger.info("Copied {0} to {1}".format(result_filename, output_filename))
    result_logfile = os.path.join(get_result_base_dir(workflow_info),
                                  'recon-all.log')
    log_filename = os.path.join(fsurfer.FREESURFER_BASE,
                                workflow_info['username'],
                                'results',
                                'recon_all-{0}.log'.format(workflow_info['job_id']))
    try:
        if not os.path.isfile(result_logfile):
            logger.error("Output file {0} not found".format(result_logfile))
            if not success:
                recover_logs(workflow_info['pegasus_ts'])
        else:
            shutil.copyfile(result_logfile, log_filename)
    except shutil.Error as e:
        logger.exception("Exception while copying file: {0}".format(e))
    except IOError as e:
        logger.exception("Exception while copying file: {0}".format(e))
    logger.info("Copied {0} to {1}".format(result_logfile, log_filename))
    try:
        if success:
            state = 'COMPLETED'
            logger.info("Updating workflow {0} to COMPLETED".format(workflow_info['job_id']))
        else:
            state = 'FAILED'
            logger.warning("Updating workflow {0} to FAILED".format(workflow_info['job_id']))

        job_update = "UPDATE freesurfer_interface.jobs  " \
                     "SET state = %s " \
                     "WHERE id = %s;"
        cursor.execute(job_update, [state, workflow_info['job_id']])
        logger.info("Updating run {0}".format(job_run_id))

        if walltime is None:
            walltime = 0
        if cputime is None:
            cputime = 0
        accounting_update = "UPDATE freesurfer_interface.job_run " \
                            "SET walltime = %s, " \
                            "    cputime = %s, " \
                            "    ended = CURRENT_TIMESTAMP," \
                            "    tasks_completed = tasks " \
                            "WHERE id = %s"
        cursor.execute(accounting_update, [walltime,
                                           cputime,
                                           job_run_id])

        conn.commit()
        conn.close()
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return


def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Update fsurf job info and "
                                                 "email user about job "
                                                 "completion")
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
    parser.add_argument('--id', dest='job_run_id',
                        action='store', help='job run id to use')

    args = parser.parse_args(sys.argv[1:])
    if args.success:
        process_results(args.job_run_id, success=True)
    else:
        process_results(args.job_run_id, success=False)

    sys.exit(0)

if __name__ == '__main__':
    main()
