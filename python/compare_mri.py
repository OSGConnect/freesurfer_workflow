#!/usr/bin/env python
import glob
import hashlib
import os
import sys
import tempfile
import tarfile

VOL_DIRS = ['mri', 'mri/orig', 'mri/tranforms']
SURFACE_DIRS = ['surf']
LABEL_DIRS = ['label']
ANNOTATION_DIRS = ['label']


def main():
    """
    Process
    :return: exit code (0 on success, 1 on failure)
    """
    work_dir = tempfile.mkdtemp()
    sys.stdout.write("Using {0} as scratch dir\n".format(work_dir))
    input_1 = sys.argv[1]
    input_2 = sys.argv[2]
    sys.stdout.write("Extracting tarballs from {0} and {1}\n".format(input_1,
                                                                     input_2))
    input_1_tarball = tarfile.open(input_1, 'r:*')
    input_2_tarball = tarfile.open(input_2, 'r:*')
    input_1_tarball.extractall(work_dir)
    input_2_tarball.extractall(work_dir)
    input_1_dir = input_1_tarball.members()[0].path
    input_2_dir = input_2_tarball.members()[0].path
    sys.stdout.write("Comparing volumes\n")
    for directory in VOL_DIRS:
        dir_entry_1 = os.path.join(input_1_dir, directory)
        dir_entry_2 = os.path.join(input_2_dir, directory)
        input_1_files = glob.glob(os.path.join(dir_entry_1,  "*.mgz"))
        input_2_files = glob.glob(os.path.join(dir_entry_2,  "*.mgz"))
        if len(input_1_files) != len(input_2_files):
            sys.stdout.write("Number of files in " +
                             "{0} ".format(dir_entry_1) +
                             "and {1} differ\n".format(dir_entry_2))
        for filename in input_1_files:
            input_1_file = os.path.join(dir_entry_1, filename)
            input_2_file = os.path.join(dir_entry_2, filename)
            sys.stdout.write("Comparing {0} to {1}... ".format(input_1_file,
                                                               input_2_file))
            return_code = os.system('mri_diff {0} {1}'.format(input_1_file,
                                                              input_2_file))
            signal = return_code & 0x00FF
            if signal != 0:
                sys.stdout.write("Signal {0} occurred\n".format(signal))
            else:
                exit_code = (return_code & 0xFF00) >> 8
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
    sys.stdout.write("Comparing surfaces\n")
    for directory in SURFACE_DIRS:
        dir_entry_1 = os.path.join(input_1_dir, directory)
        dir_entry_2 = os.path.join(input_2_dir, directory)
        input_1_files = os.listdir(dir_entry_1)
        input_2_files = os.listdir(dir_entry_2)
        if len(input_1_files) != len(input_2_files):
            sys.stdout.write("Number of files in " +
                             "{0} ".format(dir_entry_1) +
                             "and {1} differ\n".format(dir_entry_2))
        for filename in input_1_files:
            input_1_file = os.path.join(dir_entry_1, filename)
            input_2_file = os.path.join(dir_entry_2, filename)
            sys.stdout.write("Comparing {0} to {1}... ".format(input_1_file,
                                                               input_2_file))
            return_code = os.system('mris_diff {0} {1}'.format(input_1_file,
                                                               input_2_file))
            signal = return_code & 0x00FF
            if signal != 0:
                sys.stdout.write("Signal {0} occurred\n".format(signal))
            else:
                exit_code = (return_code & 0xFF00) >> 8
                if exit_code == 0:
                    sys.stdout.write("OK\n")
                else:
                    sys.stdout.write("Files differ, "
                                     "exit code: {0}\n".format(exit_code))
    sys.stdout.write("Comparing labels\n")
    for directory in LABEL_DIRS:
        dir_entry_1 = os.path.join(input_1_dir, directory)
        dir_entry_2 = os.path.join(input_2_dir, directory)
        input_1_files = glob.glob(os.path.join(dir_entry_1,  "*.label"))
        input_2_files = glob.glob(os.path.join(dir_entry_2,  "*.label"))
        if len(input_1_files) != len(input_2_files):
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
                sys.stdout.write("Two files differ\n")
            else:
                sys.stdout.write("OK")
    sys.stdout.write("Comparing annotations\n")
    for directory in LABEL_DIRS:
        dir_entry_1 = os.path.join(input_1_dir, directory)
        dir_entry_2 = os.path.join(input_2_dir, directory)
        input_1_files = glob.glob(os.path.join(dir_entry_1,  "*.annot"))
        input_2_files = glob.glob(os.path.join(dir_entry_2,  "*.annot"))
        if len(input_1_files) != len(input_2_files):
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
                sys.stdout.write("Two files differ\n")
            else:
                sys.stdout.write("OK")
    return 0

if __name__ == '__main__':
    sys.exit(main())
