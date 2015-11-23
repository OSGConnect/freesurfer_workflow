#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license
import sys
import os
import argparse
import time

import Pegasus.DAX3

SCRIPT_DIR = os.path.abspath("../bash")
CORE_ARRAY = [1, 2, 4, 8]


def generate_dax():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    errors = False
    parser = argparse.ArgumentParser(description="generate a pegasus workflow to test queue times for single and "
                                                 "multicore jobs")
    parser.add_argument('--jobs', default=50, dest='num_jobs', type=int,
                        help='number of jobs to submit per core level')
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])

    dax = Pegasus.DAX3.ADAG('queue_time_testing')
    # setup data file locations

    for cores in CORE_ARRAY:
        errors &= create_job(dax, args.num_jobs, cores)
    if not errors:  # no problems while generating DAX
        curr_date = time.strftime("%Y%m%d_%H%M%S", time.gmtime(time.time()))
        dax_name = "queue_times_{0}.xml".format(curr_date)
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
    return errors


def create_job(dax, num_jobs=50, num_cores=1):
    """
    Create a given number  of jobs that use a specified number of cores

    :param dax: Pegasus ADAG
    :param num_jobs: number of jobs to submit
    :param num_cores: number of cores to use
    :return: True if errors occurred, False otherwise
    """
    errors = False
    sleep_job = Pegasus.DAX3.Executable(name="sleep.sh",
                                        arch="x86_64",
                                        installed=False)
    sleep_job.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR,
                                                                       "sleep.sh")),
                                      "local"))
    if not dax.hasExecutable(sleep_job):
        dax.addExecutable(sleep_job)

    for x in range(0, num_jobs):
        sleep_job = Pegasus.DAX3.Job(name="sleep_{0}_{1}".format(num_cores, x))
        sleep_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR,
                                              "request_cpus",
                                              num_cores))
        dax.addJob(sleep_job)
    return errors


if __name__ == '__main__':
    failed = generate_dax()
    sys.exit(int(failed))
