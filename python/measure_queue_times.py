#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license
import sys
import os
import argparse
import time

import Pegasus.DAX3

SCRIPT_DIR = os.path.abspath("../bash")


def generate_dax():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    errors = False
    parser = argparse.ArgumentParser(description="generate a pegasus workflow to test queue times")
    parser.add_argument('--nCore', dest='num_cores', default=2, type=int,
                        help='number of cores to request')
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])

    dax = Pegasus.DAX3.ADAG('queue_testing')
    # setup data file locations

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
            errors &= create_single_job(dax, args, dax_subject_file, subject)
        elif args.serial_job:
            # setup autorecon1 run
            if not args.skip_recon:
                errors &= create_initial_job(dax, args, dax_subject_file, subject)
            errors &= create_recon2_job(dax, args, subject)
            errors &= create_final_job(dax, args, subject)
        else:
            # setup autorecon1 run
            if not args.skip_recon:
                errors &= create_initial_job(dax, args, dax_subject_file, subject)
            errors &= create_hemi_job(dax, args, 'rh', subject)
            errors &= create_hemi_job(dax, args, 'lh', subject)
            errors &= create_final_job(dax, args, subject)
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


def create_single_job(dax, args, subject_file, subject):
    """

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject_file: pegasus File object pointing to the subject mri file
    :param subject: name of subject being processed
    :return: exit code (0 for success, 1 for failure)
    :return: True if errors occurred, False otherwise
    """
    errors = False

    full_recon = Pegasus.DAX3.Executable(name="autorecon-all.sh",
                                         arch="x86_64",
                                         installed=False)
    full_recon.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR,
                                                                        "autorecon-all.sh")),
                                       "local"))
    if not dax.hasExecutable(full_recon):
        dax.addExecutable(full_recon)
    full_recon_job = Pegasus.DAX3.Job(name="autorecon-all.sh".format(subject))
    full_recon_job.addArguments(subject, subject_file, str(args.num_cores))
    full_recon_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(subject))
    full_recon_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    full_recon_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    full_recon_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", args.num_cores))
    dax.addJob(full_recon_job)
    return errors


def create_recon2_job(dax, args, subject):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    errors = False

    recon2 = Pegasus.DAX3.Executable(name="autorecon2-whole.sh",
                                         arch="x86_64",
                                         installed=False)
    recon2.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR,
                                                                        "autorecon2-whole.sh")),
                                       "local"))
    if not dax.hasExecutable(recon2):
        dax.addExecutable(recon2)
    recon2_job = Pegasus.DAX3.Job(name="autorecon2-whole.sh".format(subject))
    recon2_job.addArguments(subject, str(args.num_cores))
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(subject))
    recon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_output.tar.gz".format(subject))
    recon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    recon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    recon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", args.num_cores))
    dax.addJob(recon2_job)
    return errors

def create_initial_job(dax, args, subject_file, subject):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject_file: pegasus File object pointing to the subject mri file
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    errors = False

    autorecon_one = Pegasus.DAX3.Executable(name="autorecon1.sh", arch="x86_64", installed=False)
    autorecon_one.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon1.sh")), "local"))
    if not dax.hasExecutable(autorecon_one):
        dax.addExecutable(autorecon_one)

    autorecon1_job = Pegasus.DAX3.Job(name="autorecon1.sh")
    autorecon1_job.addArguments(subject, subject_file, str(args.num_cores))
    autorecon1_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(subject))
    autorecon1_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=False)
    dax.addJob(autorecon1_job)

    return errors


def create_hemi_job(dax, args, hemisphere, subject):
    """
    Set up job for processing a given hemisphere

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param hemisphere: hemisphere to process (should be rh or lh)
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    errors = False
    autorecon_two = Pegasus.DAX3.Executable(name="autorecon2.sh", arch="x86_64", installed=False)
    autorecon_two.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon2.sh")), "local"))
    if not dax.hasExecutable(autorecon_two):
        dax.addExecutable(autorecon_two)
    current_dir = os.getcwd()
    if hemisphere not in ['rh', 'lh']:
        return True
    autorecon2_job = Pegasus.DAX3.Job(name="autorecon2.sh")
    autorecon2_job.addArguments(subject, hemisphere, str(args.num_cores))
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(subject))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_{1}_output.tar.gz".format(subject, hemisphere))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=False)
    autorecon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    autorecon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", args.num_cores))
    dax.addJob(autorecon2_job)

    return errors


def create_final_job(dax, args, subject):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    errors = False
    autorecon_three = Pegasus.DAX3.Executable(name="autorecon3.sh", arch="x86_64", installed=False)
    autorecon_three.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon3.sh")), "local"))
    if not dax.hasExecutable(autorecon_three):
        dax.addExecutable(autorecon_three)
    autorecon3_job = Pegasus.DAX3.Job(name="autorecon3.sh")
    autorecon3_job.addArguments(subject, str(args.num_cores))
    if args.serial_job:
        recon2_output = Pegasus.DAX3.File("{0}_recon2_output.tar.gz".format(subject))
        autorecon3_job.uses(recon2_output, link=Pegasus.DAX3.Link.INPUT)
    else:
        lh_output = Pegasus.DAX3.File("{0}_recon2_lh_output.tar.gz".format(subject))
        autorecon3_job.uses(lh_output, link=Pegasus.DAX3.Link.INPUT)
        rh_output = Pegasus.DAX3.File("{0}_recon2_rh_output.tar.gz".format(subject))
        autorecon3_job.uses(rh_output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(subject))
    autorecon3_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    dax.addJob(autorecon3_job)
    return errors


if __name__ == '__main__':
    failed = generate_dax()
    sys.exit(int(failed))