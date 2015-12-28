#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license
import sys
import os
import argparse
import time


import Pegasus.DAX3
import fsurfer


def generate_dax():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    errors = False
    parser = argparse.ArgumentParser(description="generate a pegasus workflow")
    parser.add_argument('--subject', dest='subject', default=None, required=True,
                        help='Subject id(s) to process (e.g. --subject 182,64,43)')
    parser.add_argument('--cores', dest='num_cores', default=2, type=int,
                        help='number of cores to use')
    parser.add_argument('--skip-recon', dest='skip_recon',
                        action='store_true',
                        help='Skip recon processing')
    parser.add_argument('--single-job', dest='single_job',
                        action='store_true',
                        help='Do all processing in a single job')
    parser.add_argument('--serial-job', dest='serial_job',
                        action='store_true',
                        help='Do all processing as a serial workflow')
    parser.add_argument('--hemi', dest='hemisphere', default=None,
                        choices=['rh', 'lh'],
                        help='hemisphere to process (rh or lh)')
    parser.add_argument('--log', dest='logfile', default=None,
                        help='Filename to use for logging')
    parser.add_argument('--subject_dir', dest='subject_dir', default=None,
                        required=True, help='Directory with subject data files (mgz)')
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])

    dax = Pegasus.DAX3.ADAG('freesurfer')
    # setup data file locations
    subjects = args.subject.split(',')
    subject_dir = args.subject_dir

    for subject in subjects:
        subject_file = os.path.join(subject_dir, "{0}_defaced.mgz".format(subject))
        subject_file = os.path.abspath(subject_file)
        if not os.path.isfile(subject_file):
            sys.stderr.write("{0} is not present and is needed, exiting".format(subject_file))
            return True
        dax_subject_file = Pegasus.DAX3.File("{0}_defaced.mgz".format(subject))
        dax_subject_file.addPFN(Pegasus.DAX3.PFN("file://{0}".format(subject_file), "local"))
        dax.addFile(dax_subject_file)

        if args.single_job:
            errors &= fsurfer.create_single_job(dax, args, dax_subject_file, subject)
        elif args.serial_job:
            # setup autorecon1 run
            if not args.skip_recon:
                errors &= fsurfer.create_initial_job(dax, args, dax_subject_file, subject)
            errors &= fsurfer.create_recon2_job(dax, args, subject)
            errors &= fsurfer.create_final_job(dax, args, subject, serial_job=True)
        else:
            # setup autorecon1 run
            if not args.skip_recon:
                errors &= fsurfer.create_initial_job(dax, args, dax_subject_file, subject)
            errors &= fsurfer.create_hemi_job(dax, args, 'rh', subject)
            errors &= fsurfer.create_hemi_job(dax, args, 'lh', subject)
            errors &= fsurfer.create_final_job(dax, args, subject)
    if not errors:  # no problems while generating DAX
        curr_date = time.strftime("%Y%m%d_%H%M%S", time.gmtime(time.time()))
        if args.single_job:
            dax_name = "single_dax_{0}.xml".format(curr_date)
        elif args.serial_job:
            dax_name = "serial_dax_{0}.xml".format(curr_date)
        else:
            dax_name = "diamond_dax_{0}.xml".format(curr_date)
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
    return errors


if __name__ == '__main__':
    failed = generate_dax()
    sys.exit(int(failed))
