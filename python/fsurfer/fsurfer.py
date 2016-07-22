#!/usr/bin/env python

import os

import Pegasus.DAX3

SCRIPT_DIR = os.path.abspath("/usr/share/fsurfer/scripts")


def create_single_job(dax, version, cores, subject_files, subject):
    """
    Create a workflow with a single job that runs entire freesurfer workflow

    :param dax: Pegasus ADAG
    :param version: version of Freesurfer to use
    :param cores: number of cores to use
    :param subject_files: list egasus File object pointing to the subject mri files
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
    full_recon_job.addArguments(version, subject, str(cores))
    for subject_file in subject_files:
        full_recon_job.addArguments(subject_file)
        full_recon_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(subject))
    full_recon_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    full_recon_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    full_recon_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(full_recon_job)
    return errors


def create_custom_job(dax, version, cores, subject_file, subject, options):
    """
    Create a workflow with a single job that runs freesurfer workflow
    with custom options

    :param dax: Pegasus ADAG
    :param version: version of Freesurfer to use
    :param cores: number of cores to use
    :param subject_file: pegasus File object pointing tarball containing
                         subject dir
    :param subject: name of subject being processed
    :param options: options to FreeSurfer
    :return: exit code (0 for success, 1 for failure)
    :return: False if errors occurred, True otherwise
    """
    freesurfer = Pegasus.DAX3.Executable(name="freesurfer_process.sh",
                                         arch="x86_64",
                                         installed=False)
    freesurfer.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR,
                                                                        "freesurfer_process.sh")),
                                       "local"))
    if not dax.hasExecutable(freesurfer):
        dax.addExecutable(freesurfer)
    custom_job = Pegasus.DAX3.Job(name="freesurfer_process.sh".format(subject))
    custom_job.addArguments(version, subject, subject_file, str(cores), options)
    custom_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.gz".format(subject))
    custom_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    custom_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    custom_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(custom_job)
    return True


def create_recon2_job(dax, version, cores, subject):
    """
    Set up jobs for the autorecon2 process for freesurfer

    :param dax: Pegasus ADAG
    :param version: version of Freesurfer to use
    :param cores: number of cores to use
    :param subject: name of subject being processed
    :return: True if errors occurred, the pegasus job otherwise
    """
    recon2 = Pegasus.DAX3.Executable(name="autorecon2-whole.sh",
                                     arch="x86_64",
                                     installed=False)
    recon2.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR,
                                                                    "autorecon2-whole.sh")),
                                   "local"))
    if not dax.hasExecutable(recon2):
        dax.addExecutable(recon2)
    recon2_job = Pegasus.DAX3.Job(name="autorecon2-whole.sh".format(subject))
    recon2_job.addArguments(version, subject, str(cores))
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.xz".format(subject))
    recon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_output.tar.xz".format(subject))
    recon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    recon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    recon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    return recon2_job


def create_initial_job(dax, version, subject_files, subject):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param version: version of Freesurfer to use
    :param subject_files: list of pegasus File objects pointing to the subject mri files
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    autorecon_one = Pegasus.DAX3.Executable(name="autorecon1.sh", arch="x86_64", installed=False)
    autorecon_one.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon1.sh")), "local"))
    if not dax.hasExecutable(autorecon_one):
        dax.addExecutable(autorecon_one)
    autorecon1_job = Pegasus.DAX3.Job(name="autorecon1.sh")
    # autorecon1 doesn't get any benefit from more than one core
    autorecon1_job.addArguments(version, subject, '1')
    for subject_file in subject_files:
        autorecon1_job.addArguments(subject_file)
        autorecon1_job.uses(subject_file, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.xz".format(subject))
    autorecon1_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=False)
    return autorecon1_job


def create_hemi_job(dax, version, cores, hemisphere, subject):
    """
    Set up job for processing a given hemisphere

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param hemisphere: hemisphere to process (should be rh or lh)
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    autorecon_two = Pegasus.DAX3.Executable(name="autorecon2.sh", arch="x86_64", installed=False)
    autorecon_two.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon2.sh")), "local"))
    if not dax.hasExecutable(autorecon_two):
        dax.addExecutable(autorecon_two)
    if hemisphere not in ['rh', 'lh']:
        return True
    autorecon2_job = Pegasus.DAX3.Job(name="autorecon2.sh")
    autorecon2_job.addArguments(version, subject, hemisphere, str(cores))
    output = Pegasus.DAX3.File("{0}_recon1_output.tar.xz".format(subject))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_recon2_{1}_output.tar.xz".format(subject, hemisphere))
    autorecon2_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=False)
    autorecon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_memory", "4G"))
    autorecon2_job.addProfile(Pegasus.DAX3.Profile(Pegasus.DAX3.Namespace.CONDOR, "request_cpus", cores))
    return autorecon2_job


def create_final_job(dax, version, subject, serial_job=False):
    """
    Set up jobs for the autorecon3 process for freesurfer

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param subject: name of subject being processed
    :param serial_job: boolean indicating whether this is a serial workflow or not
    :return: True if errors occurred, False otherwise
    """
    autorecon_three = Pegasus.DAX3.Executable(name="autorecon3.sh", arch="x86_64", installed=False)
    autorecon_three.addPFN(Pegasus.DAX3.PFN("file://{0}".format(os.path.join(SCRIPT_DIR, "autorecon3.sh")), "local"))
    if not dax.hasExecutable(autorecon_three):
        dax.addExecutable(autorecon_three)
    autorecon3_job = Pegasus.DAX3.Job(name="autorecon3.sh")
    # only use one core on final job, more than 1 core doesn't help things
    autorecon3_job.addArguments(version, subject, '1')
    if serial_job:
        recon2_output = Pegasus.DAX3.File("{0}_recon2_output.tar.xz".format(subject))
        autorecon3_job.uses(recon2_output, link=Pegasus.DAX3.Link.INPUT)
    else:
        lh_output = Pegasus.DAX3.File("{0}_recon2_lh_output.tar.xz".format(subject))
        autorecon3_job.uses(lh_output, link=Pegasus.DAX3.Link.INPUT)
        rh_output = Pegasus.DAX3.File("{0}_recon2_rh_output.tar.xz".format(subject))
        autorecon3_job.uses(rh_output, link=Pegasus.DAX3.Link.INPUT)
    output = Pegasus.DAX3.File("{0}_output.tar.bz2".format(subject))
    logs = Pegasus.DAX3.File("recon-all.log".format(subject))
    autorecon3_job.uses(output, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    autorecon3_job.uses(logs, link=Pegasus.DAX3.Link.OUTPUT, transfer=True)
    return autorecon3_job


def create_serial_workflow(dax, version, cores, subject_file, subject,
                           skip_recon=False):
    """
    Create a workflow that processes MRI images using a serial workflow
    E.g. autorecon1 -> autorecon2 -> autorecon3

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_file: pegasus File object pointing to the subject mri file
    :param subject: name of subject being processed
    :param skip_recon: True to skip initial recon1 step
    :return: True if errors occurred, False otherwise
    """
    # setup autorecon1 run
    if not skip_recon:
        initial_job = create_initial_job(dax, version, subject_file, subject)
        if initial_job is True:
            return True
        dax.addJob(initial_job)
    recon2_job = create_recon2_job(dax, version, cores, subject)
    if recon2_job is True:
        return True
    dax.addJob(recon2_job)
    dax.addDependency(Pegasus.DAX3.Dependency(parent=initial_job, child=recon2_job))
    final_job = create_final_job(dax, version, subject, serial_job=True)
    if final_job is True:
        return True
    dax.addJob(final_job)
    dax.addDependency(Pegasus.DAX3.Dependency(parent=recon2_job, child=final_job))
    return False


def create_single_workflow(dax, version, cores, subject_files, subject):
    """
    Create a workflow that processes MRI images using a single job

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_files: list of pegasus File object pointing to the
                          subject mri files
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    return create_single_job(dax, version, cores, subject_files, subject)


def create_diamond_workflow(dax, version, cores, subject_files, subject,
                            skip_recon=False):
    """
    Create a workflow that processes MRI images using a diamond workflow
    E.g. autorecon1 -->   autorecon2-lh --> autorecon3
                     \->  autorecon2-rh /
    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_files: list of pegasus File object pointing to the subject mri files
    :param subject: name of subject being processed
    :param skip_recon: True to skip initial recon1 step
    :return: False if errors occurred, True otherwise
    """
    # setup autorecon1 run
    if not skip_recon:
        initial_job = create_initial_job(dax, version, subject_files, subject)
        if not initial_job:
            return False
        dax.addJob(initial_job)
    recon2_rh_job = create_hemi_job(dax, version, cores, 'rh', subject)
    if not recon2_rh_job:
        return False
    dax.addJob(recon2_rh_job)
    dax.addDependency(Pegasus.DAX3.Dependency(parent=initial_job, child=recon2_rh_job))
    recon2_lh_job = create_hemi_job(dax, version, cores, 'lh', subject)
    if not recon2_lh_job:
        return False
    dax.addJob(recon2_lh_job)
    dax.addDependency(Pegasus.DAX3.Dependency(parent=initial_job, child=recon2_lh_job))
    final_job = create_final_job(dax, version, subject)
    if not final_job:
        return False
    dax.addJob(final_job)
    dax.addDependency(Pegasus.DAX3.Dependency(parent=recon2_rh_job, child=final_job))
    dax.addDependency(Pegasus.DAX3.Dependency(parent=recon2_lh_job, child=final_job))
    return True


def create_custom_workflow(dax, version, cores, subject_file, subject, options):
    """
    Create a workflow that processes MRI images using custom options to
    FreeSurfer
    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_file: pegasus File object pointing to the subject dir
    :param subject: name of subject being processed
    :param options: Options to use in the workflow
    :return: False if errors occurred, True otherwise
    """
    return create_custom_job(dax, version, cores, subject_file, subject, cores, options)
