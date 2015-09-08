#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license
import sys
import os
import argparse

import Pegasus.DAX3


def generate_dax():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    errors = False
    parser = argparse.ArgumentParser(description="generate a pegasus workflow")
    parser.add_argument('--Sub', dest='subject', default=None, required=True,
                        help='Subject id to process')
    parser.add_argument('--nCore', dest='num_cores', default=2, type=int,
                        help='number of cores to use')
    parser.add_argument('--SkipRecon', dest='skip_recon',
                        action='store_true',
                        help='Skip recon processing')
    parser.add_argument('--single-job', dest='single_job',
                        action='store_true',
                        help='Do all processing in a single job')
    parser.add_argument('--tokenID', dest='token_id', default=None,
                        help='Token id to process')
    parser.add_argument('--hemi', dest='hemisphere', default=None,
                        choices=['rh', 'lh'],
                        help='hemisphere to process (rh or lh)')
    parser.add_argument('--log', dest='logfile', default=None,
                        help='Filename to use for logging')
    parser.add_argument('--subject_dir', dest='subject_dir', default=None,
                        required=True, help='Directory with subject data files (mgz)')
    parser.add_argument('--debug', dest='debug',
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])


    dax = Pegasus.DAX3.ADAG('freesurfer')
    current_dir = os.getcwd()
    # setup freesurfer scripts in dax
    autorecon_two = Pegasus.DAX3.Executable(name="autorecon2.sh", arch="x86_64", installed=False)
    autorecon_two.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(current_dir, "autorecon2.sh")), "local"))
    dax.addExecutable(autorecon_two)
    autorecon_three = Pegasus.DAX3.Executable(name="autorecon3.sh", arch="x86_64", installed=False)
    autorecon_three.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(current_dir, "autorecon3.sh")), "local"))
    dax.addExecutable(autorecon_three)

    # setup data file locations
    subject_dir = args.subject_dir
    subject_file = os.path.join(subject_dir, "{0}_defaced.mgz".format(args.subject))
    subject_file = os.path.abspath(subject_file)
    if not os.path.isfile(subject_file):
        sys.stderr.write("{0} is not present and is needed, exiting".format(subject_file))
        return True
    dax_subject_file = Pegasus.DAX3.File(subject_file)
    print("file://{0}".format(subject_file))
    dax_subject_file.addPFN(Pegasus.DAX3.PFN("file://{0}".format(subject_file)))
    dax.addFile(dax_subject_file)

    if args.single_job:
        errors &= create_single_job(dax, args, dax_subject_file)
    else:
        # setup autorecon1 run
        if not args.skip_recon:
            errors &= create_initial_job(dax, args, dax_subject_file)
        errors &= create_hemi_job(dax, args, 'rh', dax_subject_file)
        errors &= create_hemi_job(dax, args, 'lh', dax_subject_file)
        errors &= create_final_job(dax, args, dax_subject_file)
    if not errors:  # no problems while generating DAX
        with open("dax.xml", 'w') as f:
            dax.writeXML(f)
    return errors


def create_single_job(dax, args, subject_file):
    """

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject_file: pegasus File object pointing to the subject mri file
    :return: exit code (0 for success, 1 for failure)
    :return: True if errors occurred, False otherwise
    """
    errors = False

    current_dir = os.getcwd()
    full_recon = Pegasus.DAX3.Executable(name="autorecon-all.sh", arch="x86_64", installed=False)
    full_recon.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(current_dir, "autorecon-all.sh")), "local"))
    dax.addExecutable(full_recon)
    full_recon_job = Pegasus.DAX3.Job(name="autorecon-all")
    full_recon_job.addArguments(args.subject, subject_file, args.num_cores - 1)
    full_recon_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(args.subject))
    full_recon_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT)
    return errors


def create_initial_job(dax, args, subject_file):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject_file: pegasus File object pointing to the subject mri file
    :return: True if errors occurred, False otherwise
    """
    errors = False

    current_dir = os.getcwd()
    autorecon_one = Pegasus.DAX3.Executable(name="autorecon1.sh", arch="x86_64", installed=False)
    autorecon_one.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(current_dir, "autorecon1.sh")), "local"))
    dax.addExecutable(autorecon_one)
    autorecon1_job = Pegasus.DAX3.Job(name="autorecon1")
    autorecon1_job.addArguments(args.subject, subject_file, args.num_cores - 1)
    autorecon1_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(args.subject))
    autorecon1_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT)
    dax.addJob(autorecon1_job)

    return errors


def create_hemi_job(dax, args, hemisphere, subject_file):
    """
    Set up job for processing a given hemisphere

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param hemisphere: hemisphere to process (should be rh or lh)
    :param subject_file: pegasus File object pointing to the subject mri file
    :return: True if errors occurred, False otherwise
    """
    errors = False

    current_dir = os.getcwd()
    if hemisphere not in ['rh', 'lh']:
        return True
    autorecon_two = Pegasus.DAX3.Executable(name="autorecon2.sh", arch="x86_64", installed=False)
    autorecon_two.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(current_dir, "autorecon2.sh")), "local"))
    dax.addExecutable(autorecon_two)
    autorecon2_job = Pegasus.DAX3.Job(name="autorecon2-{0}".format(hemisphere))
    autorecon2_job.addArguments(args.subject, hemisphere, args.num_cores - 1)
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(args.subject))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_output.tar.gz".format(args.subject))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT)
    dax.addJob(autorecon2_job)

    return errors


def create_final_job(dax, args, subject_file):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param args: parsed arguments from command line
    :param subject_file: pegasus File object pointing to the subject mri file
    :return: True if errors occurred, False otherwise
    """
    errors = False
    current_dir = os.getcwd()
    autorecon_one = Pegasus.DAX3.Executable(name="autorecon3.sh", arch="x86_64", installed=False)
    autorecon_one.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(current_dir, "autorecon3.sh")), "local"))
    dax.addExecutable(autorecon_one)
    autorecon3_job = Pegasus.DAX3.Job(name="autorecon3")
    autorecon3_job.addArguments(args.subject, subject_file, args.num_cores - 1)
    lh_output = Pegasus.DAX3.File("{0}_recon2_lh_output.tar.gz".format(args.subject))
    autorecon3_job.uses(lh_output, link=Pegasus.DAX3.Link.INPUT)
    rh_output = Pegasus.DAX3.File("{0}_recon2_rh_output.tar.gz".format(args.subject))
    autorecon3_job.uses(rh_output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(args.subject))
    autorecon3_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT)
    dax.addJob(autorecon3_job)

    return errors


if __name__ == '__main__':
    failed = generate_dax()
    sys.exit(int(failed))
