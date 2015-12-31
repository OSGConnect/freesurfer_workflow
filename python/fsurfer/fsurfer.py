#!/usr/bin/env python

import os

import Pegasus.DAX3

SCRIPT_DIR = os.path.abspath("/usr/share/fsurfer/scripts")


def create_single_job(dax, cores, subject_file, subject):
    """

    :param dax: Pegasus ADAG
    :param cores: number of cores to use
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
    full_recon_job.addArguments(subject, subject_file, str(cores))
    full_recon_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(subject))
    full_recon_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    full_recon_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    full_recon_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(full_recon_job)
    return errors


def create_recon2_job(dax, cores, subject):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param cores: number of cores to use
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
    recon2_job.addArguments(subject, str(cores))
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(subject))
    recon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_output.tar.gz".format(subject))
    recon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    recon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    recon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(recon2_job)
    return errors


def create_initial_job(dax, cores, subject_file, subject):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param cores: number of cores to use
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
    autorecon1_job.addArguments(subject, subject_file, str(cores))
    autorecon1_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(subject))
    autorecon1_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=False)
    dax.addJob(autorecon1_job)

    return errors


def create_hemi_job(dax, cores, hemisphere, subject):
    """
    Set up job for processing a given hemisphere

    :param dax: Pegasus ADAG
    :param cores: number of cores to use
    :param hemisphere: hemisphere to process (should be rh or lh)
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    errors = False
    autorecon_two = Pegasus.DAX3.Executable(name="autorecon2.sh", arch="x86_64", installed=False)
    autorecon_two.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon2.sh")), "local"))
    if not dax.hasExecutable(autorecon_two):
        dax.addExecutable(autorecon_two)
    if hemisphere not in ['rh', 'lh']:
        return True
    autorecon2_job = Pegasus.DAX3.Job(name="autorecon2.sh")
    autorecon2_job.addArguments(subject, hemisphere, str(cores))
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.gz".format(subject))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_{1}_output.tar.gz".format(subject, hemisphere))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=False)
    autorecon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    autorecon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(autorecon2_job)

    return errors


def create_final_job(dax, cores, subject, serial_job):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param cores: number of cores to use
    :param subject: name of subject being processed
    :param serial_job: boolean indicating whether this is a serial workflow or not
    :return: True if errors occurred, False otherwise
    """
    errors = False
    autorecon_three = Pegasus.DAX3.Executable(name="autorecon3.sh", arch="x86_64", installed=False)
    autorecon_three.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon3.sh")), "local"))
    if not dax.hasExecutable(autorecon_three):
        dax.addExecutable(autorecon_three)
    autorecon3_job = Pegasus.DAX3.Job(name="autorecon3.sh")
    autorecon3_job.addArguments(subject, str(cores))
    if serial_job:
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