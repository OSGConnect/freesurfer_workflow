#!/bin/env python
# Polls the Condor collector for fsurf jobs and outputs stats for graphite

import socket
import time
import sys

import htcondor  # requires condor 7.9.5+

JOB_STATUS = {0: 'Unexpanded',
              1: 'Idle',
              2: 'Running',
              3: 'Removed',
              4: 'Completed',
              5: 'Held',
              6: 'Submission Error'}

# job attributes to retrieve
JOB_ATTRS = {'JobStatus': int,
             'User': str,
             'JobUniverse': int,
             'ProjectName': str,
             'Cmd': str}


def get_local_schedds(schedd_base_name=socket.getfqdn()):
    """
    Gets the local schedds and returns classads for them
    :param schedd_base_name: string that schedds must have, defaults to fqdn
    :return:  a list of classads for local schedds
    """
    schedd_list = []
    temp_list = htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd)
    for schedd in temp_list:
        if 'Name' not in schedd:
            continue
        if schedd_base_name in schedd['Name']:
            schedd_list.append(schedd)
    return schedd_list


def get_schedd_jobs(schedd_classad=None, job_attrs=JOB_ATTRS.keys()):
    """
    Queries local schedd to get job classads
    :param schedd_classad: classad for schedd to query
    :param job_attrs: job attributes to save from classads
    :return: a list of dicts containing job classads
    """
    job_records = []
    schedd = htcondor.Schedd(schedd_classad)
    jobs = schedd.query("True", job_attrs)
    for job in jobs:
        status = JOB_STATUS[job['JobStatus']]
        job_record = {}
        for attr in JOB_ATTRS.keys():
            try:
                if JOB_ATTRS[attr] == str:
                    job_record[attr] = str(job[attr])
                elif JOB_ATTRS[attr] == long:
                    job_record[attr] = long(job[attr])
                elif JOB_ATTRS[attr] == int:
                    job_record[attr] = int(job[attr])
                else:
                    job_record[attr] = job[attr]
            except ValueError:
                continue
            except KeyError:
                # a lot of attributes will be missing if job is not running
                pass
        job_record['JobStatus'] = status
        job_records.append(job_record)
    return job_records


def main():
    """Get and print fsurf workflow counts"""
    # initialize values
    workflows = 0
    autorecon1_idle = 0
    autorecon1_running = 0
    autorecon1_held = 0
    autorecon2_idle = 0
    autorecon2_running = 0
    autorecon2_held = 0
    autorecon3_idle = 0
    autorecon3_running = 0
    autorecon3_held = 0
    custom_idle = 0
    custom_running = 0
    custom_held = 0

    schedds = get_local_schedds()
    for schedd in schedds:
        jobs = get_schedd_jobs(schedd)
        timestamp = str(int(time.time()))
        for job in jobs:
            if not job['User'].startswith('fsurf'):
                continue
            if job['JobUniverse'] == 7 and \
                    'pegasus-dagman' in job['Cmd']:
                workflows += 1
                continue
            if 'freesurfer-process.sh' in job['Cmd']:
                if job['JobStatus'] == 'Idle':
                    custom_idle += 1
                elif job['JobStatus'] == 'Running':
                    custom_running += 1
                elif job['JobStatus'] == 'Held':
                    custom_held += 1
            if 'autorecon1.sh' in job['Cmd']:
                if job['JobStatus'] == 'Idle':
                    autorecon1_idle += 1
                elif job['JobStatus'] == 'Running':
                    autorecon1_running += 1
                elif job['JobStatus'] == 'Held':
                    autorecon1_held += 1
            if 'autorecon2.sh' in job['Cmd']:
                if job['JobStatus'] == 'Idle':
                    autorecon2_idle += 1
                elif job['JobStatus'] == 'Running':
                    autorecon2_running += 1
                elif job['JobStatus'] == 'Held':
                    autorecon2_held += 1
            if 'autorecon3.sh' in job['Cmd']:
                if job['JobStatus'] == 'Idle':
                    autorecon3_idle += 1
                elif job['JobStatus'] == 'Running':
                    autorecon3_running += 1
                elif job['JobStatus'] == 'Held':
                    autorecon3_held += 1

    running_stages = autorecon1_running + autorecon2_running + \
                     autorecon3_running + custom_running
    idle_stages = autorecon1_idle + autorecon2_idle + \
                  autorecon3_idle + custom_idle
    held_stages = autorecon1_held + autorecon2_held + \
                  autorecon3_held + custom_held
    sys.stdout.write("stats.fsurf.workflows {0} {1}\n".format(workflows,
                                                              timestamp))
    sys.stdout.write("stats.fsurf.stages.running " +
                     "{0} {1}\n".format(running_stages, timestamp))
    sys.stdout.write("stats.fsurf.stages.idle {0} {1}\n".format(idle_stages,
                                                                timestamp))
    sys.stdout.write("stats.fsurf.stages.held {0} {1}\n".format(held_stages,
                                                                timestamp))

    sys.stdout.write("stats.fsurf.autorecon1.idle " +
                     "{0} {1}\n".format(autorecon1_idle, timestamp))
    sys.stdout.write("stats.fsurf.autorecon1.running " +
                     "{0} {1}\n".format(autorecon1_running, timestamp))
    sys.stdout.write("stats.fsurf.autorecon1.held " +
                     "{0} {1}\n".format(autorecon1_held, timestamp))
    sys.stdout.write("stats.fsurf.autorecon2.idle " +
                     "{0} {1}\n".format(autorecon2_idle, timestamp))
    sys.stdout.write("stats.fsurf.autorecon2.running " +
                     "{0} {1}\n".format(autorecon2_running, timestamp))
    sys.stdout.write("stats.fsurf.autorecon2.held " +
                     "{0} {1}\n".format(autorecon2_held, timestamp))
    sys.stdout.write("stats.fsurf.autorecon3.idle " +
                     "{0} {1}\n".format(autorecon3_idle, timestamp))
    sys.stdout.write("stats.fsurf.autorecon3.running " +
                     "{0} {1}\n".format(autorecon3_running, timestamp))
    sys.stdout.write("stats.fsurf.autorecon3.held " +
                     "{0} {1}\n".format(autorecon3_held, timestamp))
    sys.stdout.write("stats.fsurf.custom.idle " +
                     "{0} {1}\n".format(custom_idle, timestamp))
    sys.stdout.write("stats.fsurf.custom.running " +
                     "{0} {1}\n".format(custom_running, timestamp))
    sys.stdout.write("stats.fsurf.custom.held " +
                     "{0} {1}\n".format(custom_held, timestamp))
    sys.exit(0)

if __name__ == '__main__':
    main()
