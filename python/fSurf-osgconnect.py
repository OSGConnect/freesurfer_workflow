#!/usr/bin/env python
import argparse
import sys
import os
import getpass
import subprocess
import re
import cStringIO
import time
import shutil

import freesurfer
import Pegasus.DAX3


WORKFLOW_BASE_DIRECTORY = os.path.join('/stash/user',
                                  getpass.getuser(),
                                  'freesurfer_scratch',
                                  'freesurfer')
WORKFLOW_DIRECTORY = os.path.join(WORKFLOW_BASE_DIRECTORY,
                                  getpass.getuser(),
                                  'pegasus',
                                  'freesurfer')


def check_and_create_workflow_dir(workflow_dir=WORKFLOW_BASE_DIRECTORY):
    """
    Check for the presence of a directory to hold workflows and
    create one if necessary

    :param workflow_dir: path to directory for workflows
    :return: True if directory exists or has been created, False otherwise
    """
    try:
        if os.path.isdir(workflow_dir):
            return True
        elif os.path.exists(workflow_dir) and not os.path.isdir(workflow_dir):
            sys.stdout.write("Can't create {0} because it ".format(workflow_dir) +
                             "is already present and not a directory\n")
            return False
        else:
            sys.stdout.write("Creating directory for workflows\n")
            os.makedirs(workflow_dir, 0o700)
            return True
    except OSError:
        sys.stdout.write("Can't create {0} for workflows, "
                         "exiting...\n".format(workflow_dir))
        return False


def run_pegasus(action, **kwargs):
    """
    Run pegasus to complete the specified action and return the output from
    pegasus, keyword arguments that might be used are:
        workflow_id -- id for pegasus workflow, used for remove and status actions
        dax_location -- path to xml file with DAX, used for submit
        workflow_directory  - directory for pegasus to keep it's workflow
                              information use for submit
        conf -- path to pegasusrc config used for submit


    :param action:      a string specifying the action (remove, status, submit)
    :return:            the output from pegasus
    """
    if not check_and_create_workflow_dir():
        return 1, "Can't create workflow directory"
    if action in ['status', 'remove']:
        if 'workflow_id' not in kwargs:
            return 1, "Workflow id missing"
        workflow_location = os.path.join(WORKFLOW_DIRECTORY,
                                         kwargs['workflow_id'])
        if not os.path.isdir(workflow_location):
            return 1, "Invalid workflow id, {0} is not a directory".format(workflow_location)
    try:
        if action == 'status':
            output = subprocess.check_output(['/usr/bin/pegasus-status',
                                              '-l',
                                              workflow_location],
                                             stderr=subprocess.STDOUT)
        elif action == 'remove':
            output = subprocess.check_output(['/usr/bin/pegasus-remove',
                                              workflow_location],
                                             stderr=subprocess.STDOUT)
        elif action == 'submit':
            if 'dax' not in kwargs:
                return 1, "DAX missing"
            elif 'conf' not in kwargs:
                return 1, "config file location missing"
            elif 'workflow_directory' not in kwargs:
                return 1, "workflow directory missing"

            output = subprocess.check_output(['/usr/bin/pegasus-plan',
                                              '--sites',
                                              'condorpool',
                                              '--dir',
                                              kwargs['workflow_directory'],
                                              '--conf',
                                              kwargs['conf'],
                                              '--dax',
                                              kwargs['dax'],
                                              '--submit'],
                                             stderr=subprocess.STDOUT)
        else:
            return 1, "Invalid pegasus action"
    except subprocess.CalledProcessError, err:
        return err.returncode, err.output

    return 0, output


def print_status(workflow_id, verbose=False):
    """
    Print the status of a workflow

    :param workflow_id:  pegasus id for workflow
    :param verbose:      whether to give full workflow details or a summary
    :return:             0 on success, 1 on error
    """
    exit_code, output = run_pegasus('status', workflow_id=workflow_id)
    if exit_code != 0:
        sys.stdout.write("An error occurred while getting job status\n")
        if output[1] is not None:
            sys.stdout.write("{0}\n".format(output))
    if verbose:
        sys.stdout.write("{0}\n".format(output))
        return exit_code
    job_summary = re.search(r'Summary: (\d+) Condor jobs(.*)', output)
    if job_summary is not None:
        idle_jobs = re.search(r'I:(\d+)', job_summary.group(2))
        num_idle = 0 
        if idle_jobs is not None:
            num_idle = idle_jobs.group(1)
        running_jobs = re.search(r'R:(\d+)', job_summary.group(2))
        num_running = 0 
        if running_jobs is not None:
            num_running = running_jobs.group(1)
        held_jobs = re.search(r'H:(\d+)', job_summary.group(2))
        num_held = 0 
        if held_jobs is not None:
            num_held = held_jobs.group(1)
        sys.stdout.write("Job information:\n")
        sys.stdout.write("{0} Idle jobs\n".format(num_idle))
        sys.stdout.write("{0} Running jobs\n".format(num_running))
        sys.stdout.write("{0} Held jobs\n".format(num_held))
        sys.stdout.write("{0} Total jobs\n".format(job_summary.group(1)))
        sys.stdout.write("\n")
    in_dag_status = False
    sys.stdout.write("Workflow information: \n")
    for line in cStringIO.StringIO(output).readlines():
        if 'UNRDY' in line or 'UNREADY' in line:
            sys.stdout.write(line + "\n")
            in_dag_status = True
            continue
        if in_dag_status:
            sys.stdout.write(line + "\n")
            if 'Summary' in line:
                break
    return exit_code


def list_workflows():
    """
    List the workflows currently in the system

    :return: 0 on success, 1 on error
    """
    try:
        check_and_create_workflow_dir()
        sys.stdout.write("Current workflows:\n")
        for entry in os.listdir(WORKFLOW_DIRECTORY):
            if os.path.isdir(os.path.join(WORKFLOW_DIRECTORY, entry)):
                sys.stdout.write("{0}\n".format(entry))
        return 0
    except IOError:
        return 1


def remove_workflow(workflow_id):
    """
    Stop and remove a specified pegasus workflow

    :param workflow_id: pegasus id for workflow
    :return: 0 on success, 1 on error
    """
    workflow_location = os.path.join(WORKFLOW_DIRECTORY,
                                     workflow_id)
    exit_code, output = run_pegasus('remove', workflow_id=workflow_id)
    if exit_code == 0:
        sys.stdout.write("Workflow {0} removed successfully\n".format(workflow_id))
        job_id = re.match(r'Job (\d+.\d+) marked for removal', output)
        if job_id is not None:
            sys.stdout.write("Waiting for running jobs to be removed...\n")
            count = 0
            while True:
                time.sleep(10)
                try:
                    output = subprocess.check_output(["/usr/bin/condor_q",  
                                                      job_id.group(1)])
                except subprocess.CalledProcessError:
                    sys.stdout.write("An error occurred while checking for "
                                     "running jobs, exiting...\n")
                    return 1
                if 'pegasus-dagman' not in output:
                    break
                count += 1
                if count > 30:
                    sys.stdout.write("Can't remove job, exiting...")
                    return 1
            sys.stdout.write("Jobs removed, removing workflow directory\n")
            try:
                shutil.rmtree(workflow_location)
            except shutil.Error:
                sys.stdout.write("Can't remove directory at "
                                 "{0}, exiting...\n".format(workflow_location))
                exit_code = 1
    else:
        sys.stdout.write("Workflow {0} was not removed\n".format(workflow_id))
        sys.stdout.write("Following error occurred:\n")
        sys.stdout.write(output)
        sys.stdout.write("You may need to remove the workflow directory "
                         "at {0} manually\n".format(workflow_location))
    return exit_code


def submit_workflow(input_file, subject_name, multicore=False):
    """
    Submit a workflow to OSG for processing

    :param input_file:    path to file with MRI data in mgz format
    :param subject_name:  name of subject in the file
    :param multicore:     boolean indicating whether to use a multicore workflow or not
    :return:              0 on success, 1 on error
    """
    if multicore:
        cores = 8
    else:
        cores = 2
    if input_file is None:
        sys.stdout.write("Input file missing, exiting...\n")
        return 1
    if subject_name is None:
        sys.stdout.write("Subject name is missing, exiting...\n")
        return 1
    dax = Pegasus.DAX3.ADAG('freesurfer')
    subject_file = os.path.abspath(input_file)
    if not os.path.isfile(subject_file):
        sys.stderr.write("{0} is not present and is needed, exiting".format(subject_file))
        return 1
    dax_subject_file = Pegasus.DAX3.File("{0}_defaced.mgz".format(subject_name))
    dax_subject_file.addPFN(Pegasus.DAX3.PFN("file://{0}".format(subject_file), "local"))
    dax.addFile(dax_subject_file)
    errors = False
    errors &= freesurfer.create_initial_job(dax, cores, dax_subject_file, subject_name)
    errors &= freesurfer.create_recon2_job(dax, cores, subject_name)
    errors &= freesurfer.create_final_job(dax, cores, subject_name, serial_job=True)
    if not errors:
        curr_date = time.strftime("%Y%m%d_%H%M%S", time.gmtime(time.time()))
        dax_name = "serial_dax_{0}.xml".format(curr_date)
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
        exit_code, output = run_pegasus('submit',
                                        dax="{0}".format(dax_name),
                                        conf="pegasusrc",
                                        sites="condorpool",
                                        workflow_directory=WORKFLOW_DIRECTORY)
        if exit_code != 0:
            sys.stdout("An error occurred when generating and submitting workflow, exiting...\n")
            sys.stdout.write("Error: \n")
            sys.stdout.write(output)
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
                else:
                    sys.stdout.write("Workflow submitted but could not get workflow id\n")
                break
    return errors


def get_output(workflow_id):
    """

    :param workflow_id: pegasus id for workflow
    :return: 0 on success, 1 on error
    """
    return 0


def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Process freesurfer information")
    parser.add_argument('--submit', dest='action',
                        action='store_const', const='submit',
                        help='Submit job for processing')
    parser.add_argument('--list', dest='action',
                        action='store_const', const='list',
                        help='List current jobs')
    parser.add_argument('--status', dest='action',
                        action='store_const', const='status',
                        help='Submit job for processing')
    parser.add_argument('--remove', dest='action',
                        action='store_const', const='remove',
                        help='Submit job for processing')
    parser.add_argument('--output', dest='action',
                        action='store_const', const='output',
                        help='Submit job for processing')
    parser.add_argument('--workflow-id', dest='workflow_id',
                        action='store', help='Pegasus workflow id to use')
    parser.add_argument('--subject', dest='subject', default=None,
                        help='Subject id to process ')
    parser.add_argument('--input-file', dest='input_file', default=None,
                        help='path to input file')
    parser.add_argument('--multicore', dest='multicore',
                        action='store_true',
                        help='Use 8 cores to')
    parser.add_argument('--verbose', dest='verbose', default=False,
                        action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])

    if args.action == 'status':
        status = print_status(args.workflow_id, args.verbose)
    elif args.action == 'list':
        status = list_workflows()
    elif args.action == 'remove':
        status = remove_workflow(args.workflow_id)
    elif args.action == 'submit':
        status = submit_workflow(args.input_file, args.subject, args.multicore)
    elif args.action == 'output':
        status = get_output(args.workflow_id)
    else:
        sys.stdout.write("Must specify an action, exiting...\n")
        parser.print_help()
        status = 0
    sys.exit(status)

if __name__ == '__main__':
    # workaround missing subprocess.check_ouput
    if "check_output" not in dir(subprocess): # duck punch it in!
        def check_output(*popenargs, **kwargs):
            r"""Run command with arguments and return its output as a byte string.

            Backported from Python 2.7 as it's implemented as pure python on stdlib.

            """
            process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
            output, unused_err = process.communicate()
            retcode = process.poll()
            if retcode:
                cmd = kwargs.get("args")
                if cmd is None:
                    cmd = popenargs[0]
                error = subprocess.CalledProcessError(retcode, cmd)
                error.output = output
                raise error
            return output

        subprocess.check_output = check_output
    main()
