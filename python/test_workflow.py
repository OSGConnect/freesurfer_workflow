#!/usr/bin/env python
import argparse
import hashlib
import httplib
import json
import os
import sys
import time
import urllib
import urlparse
import re
import subprocess


REST_ENDPOINT = "http://postgres.ci-connect.net/freesurfer"
VERSION = '1.3.13'


def get_response(query_parameters, noun, method, endpoint=REST_ENDPOINT):
    """
    Query rest endpoint with given string and return results

    :param endpoint: url to REST endpoint
    :param query_parameters: a dictionary with key, values parameters
    :param noun: object being worked on
    :param method:  HTTP method that should be used
    :return: (status code, response from query)
    """
    url = "{0}/{2}?{1}".format(endpoint,
                               urllib.urlencode(query_parameters),
                               noun)
    parsed = urlparse.urlparse(url)
    try:
        conn = httplib.HTTPConnection(parsed.netloc)
        conn.request(method, "{0}?{1}".format(parsed.path, parsed.query))
        resp = conn.getresponse()
        return resp.status, resp.read()
    except IOError, e:  # mainly dns errors
        response = {'status': 500,
                    'result': str(e)}
        return 500, json.dumps(response)
    except httplib.HTTPException, e:
        response = {'status': 400,
                    'result': str(e)}
        return 400, json.dumps(response)


def download_output(query_parameters, noun, endpoint=REST_ENDPOINT):
    """
    Query rest endpoint with given  string and return results

    :param endpoint: url to REST endpoint
    :param query_parameters: a dictionary with key, values parameters
    :param noun: object being worked on
    :return: (status code, response from query)
    """
    url = "{0}/{2}?{1}".format(endpoint,
                               urllib.urlencode(query_parameters),
                               noun)
    parsed = urlparse.urlparse(url)
    try:
        conn = httplib.HTTPConnection(parsed.hostname)
        conn.request('GET', "{0}?{1}".format(parsed.path, parsed.query))
        resp = conn.getresponse()
        content_type = resp.getheader('content-type')
        if content_type != 'application/x-bzip2' and \
           content_type != 'text/plain':
            return resp.status, resp.read()
        content_disposition = resp.getheader('content-disposition')
        if content_type == 'application/x-bzip2':
            filename = 'fsurf_output.tar.bz2'
        elif content_type == 'text/plain':
            filename = 'recon-all.log'
        match_obj = re.search(r'filename=(.*)', content_disposition)
        if match_obj:
            filename = match_obj.group(1)
        with open(filename, 'wb') as f:
            temp = resp.read(4096)
            while temp:
                f.write(temp)
                temp = resp.read(4096)
        return resp.status, json.dumps({'status': 200,
                                        'result': "output downloaded",
                                        'filename': filename})
    except httplib.HTTPException, e:
        response = {'status': 400,
                    'result': str(e)}
        return 400, json.dumps(response)


def upload_item(query_parameters, noun, body, method, endpoint=REST_ENDPOINT):
    """
    Issue a POST request to given endpoint

    :param endpoint: url to REST endpoint
    :param query_parameters: a dictionary with key, values parameters
    :param noun: object being worked on
    :param body:  data to be sent in the body
    :param method:  HTTP method that should be used (POST, PUT)
    :return: (status code, response from query)
    """
    url = "{0}/{2}?{1}".format(endpoint,
                               urllib.urlencode(query_parameters),
                               noun)
    parsed = urlparse.urlparse(url)
    try:
        conn = httplib.HTTPConnection(parsed.hostname)
        conn.request(method, "{0}?{1}".format(parsed.path, parsed.query), body)
        resp = conn.getresponse()
        return resp.status, resp.read()
    except httplib.HTTPException, e:
        response = {'status': 400,
                    'result': str(e)}
        return 400, json.dumps(response)


def get_token(userid, password):
    """
    Generate an authentication token and timestamp
    :param userid: user id identifying account
    :param password: password for user account
    :return: timestamp, token
    """
    parameters = {'userid': userid}
    code, response = get_response(parameters, 'user/salt', 'GET', REST_ENDPOINT)
    if code != 200:
        sys.stdout.write("Can't get authentication token\n")
        return None, None
    timestamp = time.time()
    response_obj = json.loads(response)
    salt = response_obj['result']
    token = hashlib.sha256(salt + password).hexdigest()
    token = hashlib.sha256(token + str(timestamp)).hexdigest()
    return str(timestamp), token


def remove_workflow(workflow_id, username, password):
    """
    Stop and remove a specified pegasus workflow

    :param workflow_id: pegasus id for workflow
    :param username: username to use when authenticating
    :param password: password to user when authenticating
    :return: 'Success' or 'Error'
    """
    query_params = {}
    timestamp, token = get_token(username, password)
    if token is None:
        return 'Error'
    query_params['userid'] = username
    query_params['timestamp'] = timestamp
    query_params['token'] = token
    query_params['jobid'] = workflow_id
    status, response = get_response(query_params, 'job', 'DELETE')
    resp_dict = json.loads(response)
    if status != 200:
        sys.stdout.write("Error deleting "
                         "workflow:\n{0}\n".format(resp_dict['result']))
        return 'Error'
    sys.stdout.write("Workflow removed\n")
    return 'Success'


def submit_workflow(username, password, input_directory, subject_name):
    """
    Submit a workflow to OSG for processing

    :param username: username to use when authenticating
    :param password: password to user when authenticating
    :param input_directory:    path to file with MRI data in mgz format
    :param subject_name:  name of subject in the file
    :param multicore:     boolean indicating whether to use a multicore workflow or not
    :return:              job_id on success, None on error
    """
    if subject_name is None:
        sys.stdout.write("Subject name is missing, exiting...\n")
        return None
    subject_file = os.path.join(input_directory,
                                "{0}_defaced.mgz".format(subject_name))
    subject_file = os.path.abspath(subject_file)
    if not os.path.isfile(subject_file):
        sys.stderr.write("{0} is not present and is needed, exiting\n".format(subject_file))
        return None
    with open(subject_file, 'rb') as f:
        body = f.read()
    query_params = {}
    timestamp, token = get_token(username, password)
    if token is None:
        return None
    query_params['userid'] = username
    query_params['timestamp'] = timestamp
    query_params['token'] = token
    query_params['multicore'] = True
    query_params['subject'] = subject_name
    query_params['jobname'] = "{0}_{1}".format(subject_name, timestamp)
    query_params['filename'] = "{0}_defaced.mgz".format(subject_name)
    sys.stdout.write("Creating and submitting workflow\n")
    status, response = upload_item(query_params, 'job', body, 'POST')
    if status != 200:
        sys.stdout.write("Error while submitting workflow\n")
        return None
    response_obj = json.loads(response)
    job_id = response_obj['job_id']
    sys.stdout.write("Workflow {0} submitted for processing\n".format(job_id))
    return job_id


def get_output(workflow_id, username, password):
    """
    Get MRI data for a completed workflow

    :param workflow_id: pegasus id for workflow
    :param username: username to use when authenticating
    :param password: password to user when authenticating
    :return: response_obj with status and filename
    """
    query_params = {}
    timestamp, token = get_token(username, password)
    if token is None:
        return {'status': 400, 'result': 'Can\'t authenticate'}
    query_params['userid'] = username
    query_params['timestamp'] = timestamp
    query_params['token'] = token
    query_params['jobid'] = workflow_id
    sys.stdout.write("Downloading results, this may take a while\n")
    status, response = download_output(query_params, 'job/output')
    response_obj = json.loads(response)
    if status != 200:
        sys.stdout.write("Error while downloading results:\n")
        sys.stdout.write("{0}\n".format(response_obj['result']))
        return response_obj
    sys.stdout.write("Downloaded to {0}\n".format(response_obj['filename']))
    sys.stdout.write("To extract the results: tar "
                     "xvjf {0}\n".format(response_obj['filename']))
    return response_obj


def get_status(workflow_id, username, password):
    """
    Get status for a workflow

    :param workflow_id: pegasus id for workflow
    :param username: username to use when authenticating
    :param password: password to user when authenticating
    :return: job status
    """
    query_params = {}
    timestamp, token = get_token(username, password)
    if token is None:
        return 'ERROR'
    query_params['userid'] = username
    query_params['timestamp'] = timestamp
    query_params['token'] = token
    query_params['jobid'] = workflow_id
    status, response = get_response(query_params,
                                    'job/status',
                                    'GET')
    response_obj = json.loads(response)
    if status == 404:
        sys.stdout.write("Workflow with id {0} not found\n".format(workflow_id))
        return 'ERROR'
    elif status != 200:
        sys.stdout.write("Error while getting job status:\n")
        sys.stdout.write("{0}\n".format(response_obj['result']))
        return response_obj['result']
    sys.stdout.write("Current job status: {0}\n".format(response_obj['job_status']))
    return response_obj['job_status']


def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Process freesurfer information")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    parser.add_argument('--reference', dest='reference',
                        choices=['MRN_3', 'MRN_1'], help='reference image to process')
    parser.add_argument('--dir', dest='input_directory',
                        default='.', help='directory containing input file')
    parser.add_argument('--reference_output', dest='check_file',
                        default=None, help='path to reference output to compare to')
    parser.add_argument('--dualcore', dest='multicore',
                        action='store_false', default=True,
                        help='Use 2 cores to process certain steps')
    # General arguments
    parser.add_argument('--user', dest='user', default=None,
                        help='Username to use to login')
    parser.add_argument('--password', dest='password',
                        default=None, help='Password used to login')

    args = parser.parse_args(sys.argv[1:])

    job_id = submit_workflow(args.user,
                             args.password,
                             args.input_directory,
                             args.reference)
    if job_id is None:
        sys.exit(1)
    running = True
    start_time = time.time()
    error = False
    while running:
        status = get_status(job_id, args.user, args.password)
        if (status != 'PROCESSING') and (status != 'UPLOADED'):
            break

        if (time.time() - start_time) > (86400 * 2):
            sys.stdout.write("Timed out while processing\n")
            error = True
            break
        time.sleep(3600)
    if error:
        remove_workflow(job_id, args.user, args.password)
        sys.exit(1)
    response = get_output(job_id, args.user, args.password)
    if response['status'] != 200:
        remove_workflow(job_id, args.user, args.password)
        sys.exit(1)
    output_file = response['filename']
    remove_workflow(job_id, args.user, args.password)
    if args.reference == 'MRN_3':
        reference_output = "../MRN_3_reference.tbz"
    elif args.reference == 'MRN_1':
        reference_output = "../MRN_1_reference.tbz"
    try:
        subprocess.check_call(["./compare_mri.py",
                               output_file,
                               reference_output])
    except subprocess.CalledProcessError:
        sys.exit(1)
    sys.exit(0)

if __name__ == '__main__':
    main()
