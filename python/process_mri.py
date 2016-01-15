#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license
import sys
import argparse
import os
import psycopg2

def submit_workflow(input_directory, subject_name, multicore=False,
                    workflow='serial'):
    """
    Submit a workflow to OSG for processing

    :param input_directory:    path to file with MRI data in mgz format
    :param subject_name:  name of subject in the file
    :param multicore:     boolean indicating whether to use a multicore
                          workflow or not
    :param workflow:      string indicating type of workflow to run (serial,
                          diamond, single)
    :return:              0 on success, 1 on error
    """
    if multicore:
        cores = 8
    else:
        cores = 2
    if subject_name is None:
        sys.stdout.write("Subject name is missing, exiting...\n")
        return 1
    sys.stdout.write("Has the MRI data been anonymized and defaced [y/n]? ")
    response = sys.stdin.readline()
    if response.strip().lower() != 'y':
        sys.stdout.write("MRI data must be anonymized and defaced before being submitted\n")
        return 1
    sys.stdout.write("Creating and submitting workflow\n")
    subject_file = os.path.join(input_directory,
                                "{0}_defaced.mgz".format(subject_name))
    subject_file = os.path.abspath(subject_file)
    if not os.path.isfile(subject_file):
        sys.stderr.write("{0} is not ".format(subject_file) +
                         "present and is needed, exiting\n")
        return 1
    dax = Pegasus.DAX3.ADAG('freesurfer')
    dax_subject_file = Pegasus.DAX3.File("{0}_defaced.mgz".format(subject_name))
    dax_subject_file.addPFN(Pegasus.DAX3.PFN("file://{0}".format(subject_file),
                                             "local"))
    dax.addFile(dax_subject_file)
    if workflow == 'serial':
        errors = fsurfer.create_serial_workflow(dax,
                                                cores,
                                                dax_subject_file,
                                                subject_name)
    elif workflow == 'diamond':
        errors = fsurfer.create_diamond_workflow(dax,
                                                 cores,
                                                 dax_subject_file,
                                                 subject_name)
    elif workflow == 'single':
        errors = fsurfer.create_single_workflow(dax,
                                                cores,
                                                dax_subject_file,
                                                subject_name)
    else:
        sys.stdout.write("Unknown workflow specified, using serial instead\n")
        errors = fsurfer.create_serial_workflow(dax,
                                                cores,
                                                dax_subject_file,
                                                subject_name)
    if not errors:
        curr_date = time.strftime("%Y%m%d_%H%M%S", time.gmtime(time.time()))
        dax_name = "serial_dax_{0}.xml".format(curr_date)
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
        exit_code, output = run_pegasus('submit',
                                        dax="{0}".format(dax_name),
                                        conf=PEGASUSRC_PATH,
                                        sites="condorpool",
                                        workflow_directory=WORKFLOW_BASE_DIRECTORY)
        if exit_code != 0:
            sys.stdout("An error occurred when generating and submitting workflow, exiting...\n")
            sys.stdout.write("Error: \n")
            sys.stdout.write(output)
            os.unlink(dax_name)
            return 1
        capture_id = False
        for line in cStringIO.StringIO(output).readlines():
            if 'Your workflow has been started' in line:
                capture_id = True
            if capture_id and WORKFLOW_DIRECTORY in line:
                id_match = re.search(r'([T\d]+-\d+)'.format(WORKFLOW_DIRECTORY),
                                     line)
                if id_match is not None:
                    sys.stdout.write("Workflow submitted with an "
                                     "id of {0}\n".format(id_match.group(1)))
                    save_workflow_info(id_match.group(1),
                                       subject_name,
                                       multicore)
                else:
                    sys.stdout.write("Workflow submitted but could not get workflow id\n")
                break

        os.unlink(dax_name)
    return errors



def process_image():
    """
    Process image specified from command line and

    :return: exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(description="Process uploaded MRI images")
    parser.add_argument('--image', dest='image', default=None, required=True,
                        help='Name of image file with MRI data (needs to be in mgz format)')
    parser.add_argument('--username', dest='username', default=None, required=True,
                        help='Username of submitter')
    parser.add_argument('--processing_directory', dest='processing_dir',
                        help='Directory files should go into for processing', required=True)
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])

    if not os.path.isfile(args.image):
        return 1

    psycopg2.





if __name__ == '__main__':
    sys.exit(process_image())
