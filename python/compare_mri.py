#!/usr/bin/env python
import glob
import hashlib
import os
import sys
import tempfile
import tarfile
import subprocess
import shutil

VOL_DIRS = ['mri', 'mri/orig', 'mri/tranforms']
SURFACE_DIRS = ['surf']
LABEL_DIRS = ['label']
ANNOTATION_DIRS = ['label']
VOLUMES = ['rawavg.mgz',
           'orig.mgz',
           'nu.mgz',
           'T1.mgz',
           'brainmask.mgz',
           'norm.mgz',
           'aseg.mgz',
           'brain.mgz',
           'wm.mgz',
           'filled.mgz',
           'aparc+aseg.mgz',
           'lh.ribbon.mgz',
           'rh.ribbon.mgz']
SURFACES = ['orig.nofix', 'smoothwm.nofix', 'inflated.nofix',
            'qsphere.nofix', 'orig', 'smoothwm', 'inflated',
            'white', 'pial', 'sphere', 'sphere.reg']
CURVES = ['curv',
          'curv.pial',
          'sulc',
          'thickness',
          'area',
          'area.pial',
          'volume']
APARCS = ['aparc.a2009s', 'aparc']
STATS_FILES = ['aseg.stats', 'lh.aparc.a2009s.stats', 'lh.aparc.stats',
               'rh.aparc.a2009s.stats', 'rh.aparc.stats', 'wmparc.stats']


def run_command(command):
    """
    Run a command and return results
    :param command: command to run
    :return: a tuple (signal, exit code)
    """
    dev_null = open(os.devnull, 'w')
    retval = subprocess.call(command, stdout=dev_null, stderr=dev_null, shell=True)
    return get_exitcode(retval)


def get_exitcode(return_code):
    """
    Calculate and return exit code from a return code

    :param return_code: code in os.wait format
    :return: a tuple (signal, exit code)
    """

    signal = return_code & 0x00FF
    exitcode = (return_code & 0xFF00) >> 8
    return signal, exitcode


def compare_curves(subject1, subject2, subject1_dir, subject2_dir):
    """
    Compare the curves generated by Freesurfer for two inputs and
    indicate whether they are the same or different

    :param subject1: name of first subject
    :param subject2: name of second subject
    :param subject1_dir: path to files for first subject
    :param subject2_dir: path to files for second subject
    :return: True if files have different curves, False otherwise
    """
    sys.stdout.write("Comparing curves\n")
    differences = False
    for hemi in ['rh', 'lh']:
        for curve in CURVES:
                sys.stdout.write("Comparing curve {0}... ".format(curve))
                cmd = "mris_diff --s1 {0} --s2 {1} ".format(subject1, subject2)
                cmd += "--sd1 {0} --sd2 {1} ".format(subject1_dir,
                                                     subject2_dir)
                cmd += "--hemi {0} --curv {1}".format(hemi, curve)
                signal, exit_code = run_command(cmd)
                if signal != 0:
                    differences = True
                    sys.stdout.write("Signal {0} occurred\n".format(signal))
                elif exit_code == 0:
                    sys.stdout.write("OK\n")
                else:
                    differences = True
                    sys.stdout.write("Files differ, "
                                     "exit code: {0}\n".format(exit_code))
    return differences


def compare_surfaces(subject1_dir, subject2_dir):
    """
    Compare the surfaces generated by Freesurfer for two inputs and
    indicate whether they are the same or different

    :param subject1_dir: path to files for first subject
    :param subject2_dir: path to files for second subject
    :return: True if files have different surfaces, False otherwise
    """
    sys.stdout.write("Comparing surfaces\n")
    differences = False
    input_1_files = os.listdir(subject1_dir)
    input_2_files = os.listdir(subject2_dir)
    if len(input_1_files) != len(input_2_files):
        differences = True
        sys.stdout.write("Number of files in " +
                         "{0} ".format(subject1_dir) +
                         "and {1} differ\n".format(subject2_dir))

    for hemi in ['rh', 'lh']:
        for surface in SURFACES:
                sys.stdout.write("Comparing surface {0}... ".format(surface))
                surface_1 = os.path.join(subject1_dir,
                                         "surf",
                                         "{0}.{1}".format(hemi, surface))
                surface_2 = os.path.join(subject2_dir,
                                         "surf",
                                         "{0}.{1}".format(hemi, surface))
                cmd = "mris_diff --thresh 0 --maxerrs 1000 "
                cmd += "{0} {1}".format(surface_1, surface_2)
                signal, exit_code = run_command(cmd)
                if signal != 0:
                    differences = True
                    sys.stdout.write("Signal {0} occurred\n".format(signal))
                elif exit_code == 0:
                    sys.stdout.write("OK\n")
                else:
                    differences = True
                    sys.stdout.write("Files differ, "
                                     "exit code: {0}\n".format(exit_code))
    return differences


def compare_aparcs(subject1, subject2, subject1_dir, subject2_dir,
                   subjects_dir):
    """
    Compare the aparcs generated by Freesurfer for two inputs and
    indicate whether they are the same or different

    :param subject1: name of first subject
    :param subject2: name of second subject
    :param subject1_dir: path to files for first subject
    :param subject2_dir: path to files for second subject
    :param subjects_dir: path to directory with symlinks to
                         both subject dirs (used for parcellation
                         comparison)
    :return: True if files have different curves, False otherwise
    """
    sys.stdout.write("Comparing aparcs\n")
    differences = False
    os.environ['SUBJECTS_DIR'] = subjects_dir
    for hemi in ['rh', 'lh']:
        for aparc in APARCS:
                sys.stdout.write("Comparing aparc {0}... \n".format(aparc))
                sys.stdout.write("mris_diff... ".format(aparc))
                cmd = "mris_diff --s1 {0} --s2 {1} ".format(subject1, subject2)
                cmd += "--sd1 {0} --sd2 {1} ".format(subject1_dir,
                                                     subject2_dir)
                cmd += "--hemi {0} --aparc {1}".format(hemi, aparc)
                signal, exit_code = run_command(cmd)
                if signal != 0:
                    differences = True
                    sys.stdout.write("Signal {0} occurred\n".format(signal))
                elif exit_code == 0:
                    sys.stdout.write("OK\n")
                else:
                    differences = True
                    sys.stdout.write("Files differ, "
                                     "exit code: {0}\n".format(exit_code))
                sys.stdout.write("Comparing overlaps... ")
                cmd = "mri_surf2surf --srcsubject subject1 "
                cmd += "--trgsubject subject2 ".format(subject2)
                cmd += "--hemi {0} ".format(hemi)
                cmd += "--sval-annot {0}/{2}/label/{1}.{3}.annot ".format(subject1_dir,
                                                                          hemi,
                                                                          subject1,
                                                                          aparc)
                cmd += "--tval {0}/{2}/label/{1}.{2}_ref.{3}.annot ".format(subject1_dir,
                                                                            hemi,
                                                                            subject1,
                                                                            aparc)
                signal, exit_code = run_command(cmd)
                if signal != 0:
                    differences = True
                    sys.stdout.write("Signal {0} occurred\n".format(signal))
                elif exit_code != 0:
                    differences = True
                    sys.stdout.write("Error comparing parcellations, "
                                     "exit code: {0}\n".format(exit_code))
                cmd = "mris_compute_parc_overlap --sd {0} ".format(subject1_dir)
                cmd += "--s {0} --hemi {1}  ".format(subject1, hemi)
                cmd += "--annot1 {0} ".format(aparc)
                cmd += "--annot2 {0}_ref.{1} ".format(subject1, aparc)
                cmd += "--debug-overlap"
                signal, exit_code = run_command(cmd)
                if signal != 0:
                    differences = True
                    sys.stdout.write("Signal {0} occurred\n".format(signal))
                elif exit_code == 0:
                    sys.stdout.write("OK\n")
                else:
                    differences = True
                    sys.stdout.write("Error comparing parcellations, "
                                     "exit code: {0}\n".format(exit_code))

    return differences


def compare_volumes(subject1_dir, subject2_dir):
    """
    Compare the volumes generated by Freesurfer for two inputs and
    indicate whether they are the same or different

    :param subject1_dir: path to files for first subject
    :param subject2_dir: path to files for second subject
    :return: True if files have different volumes, False otherwise
    """
    differences = False
    sys.stdout.write("Comparing volumes\n")
    subj1_files = os.listdir(subject1_dir)
    subj2_files = os.listdir(subject2_dir)
    if len(subj1_files) != len(subj2_files):
        sys.stdout.write("Number of files in " +
                         "{0} ".format(subject1_dir) +
                         "and {1} differ\n".format(subject2_dir))
    for volume in VOLUMES:
            sys.stdout.write("Comparing volume {0}... ".format(volume))
            volume_1 = os.path.join(subject1_dir,
                                    "mri",
                                    "{0}".format(volume))
            volume_2 = os.path.join(subject2_dir,
                                    "mri",
                                    "{0}".format(volume))
            cmd = "mri_diff --thresh 0 "
            cmd += "{0} {1}".format(volume_1, volume_2)
            signal, exit_code = run_command(cmd)
            if signal != 0:
                differences = True
                sys.stdout.write("Signal {0} occurred\n".format(signal))
            if exit_code != 0:
                differences = True
            if exit_code == 0:
                sys.stdout.write("OK\n")
            elif exit_code == 1:
                sys.stdout.write("An error occurred\n")
            elif exit_code == 101:
                sys.stdout.write("Files differ in dimension\n")
            elif exit_code == 102:
                sys.stdout.write("Files differ in resolution\n")
            elif exit_code == 103:
                sys.stdout.write("Files differ in acquisition parameters\n")
            elif exit_code == 104:
                sys.stdout.write("Files differ in geometry\n")
            elif exit_code == 105:
                sys.stdout.write("Files differ in precision\n")
            elif exit_code == 106:
                sys.stdout.write("Files differ in pixel data\n")
            elif exit_code == 107:
                sys.stdout.write("Files differ in orientation\n")
            else:
                differences = True
                sys.stdout.write("Files differ, "
                                 "exit code: {0}\n".format(exit_code))
    sys.stdout.write("Comparing seg overlap... ")
    cmd = "mri_compute_seg_overlap {0}/mri/aseg.mgz ".format(subject1_dir)
    cmd += "{0}/mri/aseg.mgz".format(subject2_dir)
    signal, exit_code = run_command(cmd)
    if signal != 0:
        differences = True
        sys.stdout.write("Signal {0} occurred\n".format(signal))
    if exit_code == 0:
        sys.stdout.write("OK\n")
    else:
        differences = True
        sys.stdout.write("Files differ, "
                         "exit code: {0}\n".format(exit_code))
    return differences


def compare_labels(subject1_dir, subject2_dir):
    """
    Compare the labels generated by Freesurfer for two inputs and
    indicate whether they are the same or different

    :param subject1_dir: path to files for first subject
    :param subject2_dir: path to files for second subject
    :return: True if files have different labels, False otherwise
    """
    differences = False
    sys.stdout.write("Comparing labels\n")
    for directory in LABEL_DIRS:
        dir_entry_1 = os.path.join(subject1_dir, directory)
        dir_entry_2 = os.path.join(subject2_dir, directory)
        input_1_files = glob.glob(os.path.join(dir_entry_1,  "*.label"))
        input_2_files = glob.glob(os.path.join(dir_entry_2,  "*.label"))
        if len(input_1_files) != len(input_2_files):
            differences = True
            sys.stdout.write("Number of files in " +
                             "{0} ".format(dir_entry_1) +
                             "and {0} differ\n".format(dir_entry_2))
        for filename in input_1_files:
            input_1_file = os.path.join(dir_entry_1, filename)
            input_2_file = os.path.join(dir_entry_2, filename)
            sys.stdout.write("Comparing {0} to {1}... ".format(input_1_file,
                                                               input_2_file))
            input_1_md5 = hashlib.md5()
            input_1_md5.update(open(input_1_file).read())
            input_2_md5 = hashlib.md5()
            input_2_md5.update(open(input_2_file).read())
            if input_1_md5.digest() != input_2_md5.digest():
                differences = True
                sys.stdout.write("Two files differ\n")
            else:
                sys.stdout.write("OK\n")
    return differences


def compare_annotations(subject1_dir, subject2_dir):
    """
    Compare the annotations generated by Freesurfer for two inputs and
    indicate whether they are the same or different

    :param subject1_dir: path to files for first subject
    :param subject2_dir: path to files for second subject
    :return: True if files have different annotations, False otherwise
    """
    differences = False
    sys.stdout.write("Comparing annotations\n")
    for directory in LABEL_DIRS:
        dir_entry_1 = os.path.join(subject1_dir, directory)
        dir_entry_2 = os.path.join(subject2_dir, directory)
        input_1_files = glob.glob(os.path.join(dir_entry_1,  "*.annot"))
        input_2_files = glob.glob(os.path.join(dir_entry_2,  "*.annot"))
        if len(input_1_files) != len(input_2_files):
            differences = True
            sys.stdout.write("Number of files in " +
                             "{0} ".format(dir_entry_1) +
                             "and {1} differ\n".format(dir_entry_2))
        for filename in input_1_files:
            input_1_file = os.path.join(dir_entry_1, filename)
            input_2_file = os.path.join(dir_entry_2, filename)
            sys.stdout.write("Comparing {0} to {1}... ".format(input_1_file,
                                                               input_2_file))
            input_1_md5 = hashlib.md5()
            input_1_md5.update(open(input_1_file).read())
            input_2_md5 = hashlib.md5()
            input_2_md5.update(open(input_2_file).read())
            if input_1_md5.digest() != input_2_md5.digest():
                differences = True
                sys.stdout.write("Two files differ\n")
            else:
                sys.stdout.write("OK\n")
    return differences


def main(work_dir):
    """
    Compare two MRI results and list any differences

    :param work_dir:  directory to use as a working directory
    :return: exit code (0 on success, 1 on failure)
    """
    sys.stdout.write("Using {0} as scratch dir\n".format(work_dir))
    input_1 = sys.argv[1]
    input_2 = sys.argv[2]
    sys.stdout.write("Extracting tarballs from {0} and {1}\n".format(input_1,
                                                                     input_2))
    input_1_tarball = tarfile.open(input_1, 'r:*')
    input_2_tarball = tarfile.open(input_2, 'r:*')
    subject1 = input_1_tarball.getmembers()[0].path
    subject2 = input_2_tarball.getmembers()[0].path
    subject1_dir = os.path.join(work_dir, 'input1')
    subject2_dir = os.path.join(work_dir, 'input2')
    input_1_tarball.extractall(subject1_dir)
    input_2_tarball.extractall(subject2_dir)
    input_1_dir = os.path.join(subject1_dir, subject1)
    input_2_dir = os.path.join(subject2_dir, subject2)

    # setup symlinks for parcellation comparisons
    subjects_dir = os.path.join(work_dir, "subjects")
    os.mkdir(subjects_dir)
    os.symlink(input_1_dir, os.path.join(subjects_dir, 'subject1'))
    os.symlink(input_2_dir, os.path.join(subjects_dir, 'subject2'))

    # Do comparisons
    inputs_different = False
    inputs_different |= compare_labels(input_1_dir, input_2_dir)
    inputs_different |= compare_annotations(input_1_dir, input_2_dir)
    inputs_different |= compare_volumes(input_1_dir, input_2_dir)
    inputs_different |= compare_surfaces(input_1_dir, input_2_dir)
    inputs_different |= compare_curves(subject1, subject2,
                                       subject1_dir, subject2_dir)
    inputs_different |= compare_aparcs(subject1, subject2,
                                       subject1_dir, subject2_dir, 
                                       subjects_dir)
    if inputs_different:
        sys.stdout.write("Differences between the two files!")
    else:
        sys.stdout.write("Files check out!")
    return 0

if __name__ == '__main__':
    try:
        scratch_dir = tempfile.mkdtemp()
        sys.exit(main(scratch_dir))
    finally:
        shutil.rmtree(scratch_dir)

