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


REST_ENDPOINT = "http://postgres.ci-connect.net/freesurfer_test"
VERSION = 'PKG_VERSION'


def check_freesurfer():
    """
    Check to make sure freesurfer binaries are in path

    :return: True if FreeSurfer is available, false otherwise
    """
    success = False
    for path in os.environ['PATH'].split(':'):
        if os.path.isfile(os.path.join(path, 'recon-all')):
            success = True
    return success


def error_message(message):
    """
    Print an error message with default message

    :param message: error message to write
    :return: None
    """
    sys.stderr.write("{0}\n".format(message))


def get_response(query_parameters, noun, method, endpoint=REST_ENDPOINT):
    """
    Query rest endpoint with given  string and return results

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
    except IOError as e:  # mainly dns errors
        response = {'status': 500,
                    'result': str(e)}
        return 500, json.dumps(response)
    except httplib.HTTPException as e:
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
        if content_type.startswith('application/x-bzip2') and \
           content_type.startswith('text/plain'):
            return resp.status, resp.read()
        content_disposition = resp.getheader('content-disposition')
        if content_type.startswith('application/x-bzip2'):
            filename = 'fsurf_output.tar.bz2'
        elif content_type.startswith('text/plain'):
            filename = 'recon-all.log'
        else:
            response = {'status': 500,
                        'result': "Unknown content-type: "
                                  "{0}".format(content_type)}
            return 500, json.dumps(response)
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
    except httplib.HTTPException as e:
        response = {'status': 400,
                    'result': str(e)}
        return 400, json.dumps(response)


def encode_file(body, filename):
    """
    Encode a file in a binary form and return a mime content type
    and encoded binary data

    :param body: binary data to encode
    :param filename: name of file with data to encode
    :return: content_type, body
    """
    boundary = '--------------MIME_Content_Boundary---------------'
    lines = []
    lines.append('--' + boundary)
    lines.append('Content-Disposition: form-data; name="input_file"; '
                 'filename="{0}"'.format(filename))
    lines.append('Content-Type: application/octet-stream')
    lines.append('')
    lines.append(body)
    lines.append('--' + boundary + '--')
    lines.append('')
    encoded = "\r\n".join(lines)
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return content_type, encoded


def upload_item(query_parameters, noun, filename, body, method, endpoint=REST_ENDPOINT):
    """
    Issue a POST request to given endpoint

    :param endpoint: url to REST endpoint
    :param query_parameters: a dictionary with key, values parameters
    :param noun: object being worked on
    :param filename: name of file being transferred
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
        content_type, body = encode_file(body, filename)
        headers = {'content-type': content_type,
                   'Accept': 'text/plain'}
        conn.request(method,
                     "{0}?{1}".format(parsed.path, parsed.query),
                     body,
                     headers)
        resp = conn.getresponse()
        if resp.status == 401:
            # invalid password
            response = {'status': resp.status,
                        'result': 'Invalid username/password'}
            return resp.status, json.dumps(response)
        elif resp.status == 400:
            # invalid password
            response = {'status': resp.status,
                        'result': 'Invalid parameter'}
            return resp.status, json.dumps(response)

        return resp.status, resp.read()
    except httplib.HTTPException as e:
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
    if code == 401:
        error_message("User account disabled\n")
        return None, None
    elif code == 400:
        error_message("Userid not found\n")
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
    :return: exits with 0 on success, 1 on error
    """
    timestamp, token = get_token(username, password)
    if token is None:
        return 1
    query_params = {'userid': username,
                    'timestamp': timestamp,
                    'token': token,
                    'jobid': workflow_id}
    status, response = get_response(query_params, 'job', 'DELETE')
    resp_dict = json.loads(response)
    if status != 200:
        error_message("Error deleting workflow:\n" +
                      resp_dict['result'])
        return 1
    sys.stdout.write("Workflow removed\n")
    return 0


def submit_standard_workflow(username,
                             password,
                             version,
                             subject,
                             input_files,
                             multicore=True):
    """
    Submit a workflow to OSG for processing

    :param username:    username to use when authenticating
    :param password:    password to user when authenticating
    :param version:     version of FreeSurfer to use
    :param input_files: list with path to files with MRI data in mgz format
    :param subject:     name of subject in the file
    :param multicore:   boolean indicating whether to use a multicore workflow
    :return:            job_id on success, None on error
    """

    timestamp, token = get_token(username, password)
    num_inputs = len(input_files)
    query_params = {'userid': username,
                    'token': token,
                    'multicore': multicore,
                    'num_inputs': 1,
                    'options': "",
                    'version': version,
                    'subject': subject,
                    'timestamp': timestamp,
                    'jobname': "validation_{0}_{1}".format(subject, timestamp)}
    sys.stdout.write("Creating and submitting workflow\n")
    status, response = get_response(query_params, 'job', 'POST')
    if status != 200:
        response_obj = json.loads(response)
        error_message("Error while creating workflow:\n" +
                      response_obj['result'])
        sys.exit(1)
    response_obj = json.loads(response)
    job_id = response_obj['job_id']
    sys.stdout.write("Workflow {0} created\n".format(job_id))

    sys.stdout.write("Uploading input file\n")
    file_num = 0
    for input_file in input_files:
        attempts = 1
        sys.stdout.write("Uploading {0} (file {1}/{2})\n".format(input_file,
                                                                 file_num + 1,
                                                                 num_inputs))
        while attempts < 6:
            query_params = {'userid': username,
                            'timestamp': timestamp,
                            'token': token,
                            'jobid': job_id,
                            'filename': os.path.basename(input_file),
                            'subjectdir': False}
            input_path = os.path.abspath(input_file)
            if not os.path.isfile(input_path):
                sys.stderr.write("{0} is not present and is needed, "
                                 "exiting\n".format(input_path))
                sys.exit(1)
            with open(input_path, 'rb') as f:
                body = f.read()

            status, response = upload_item(query_params,
                                           'job/input',
                                           os.path.basename(input_file),
                                           body,
                                           'POST')
            if status == 200:
                sys.stdout.write("Uploaded {0} successfully\n".format(input_file))
                break
            response_obj = json.loads(response)
            sys.stdout.write("Error while uploading {0}\n".format(input_file))
            sys.stdout.write("Error: {0}\n".format(response_obj['result']))
            sys.stdout.write("Retrying upload, attempt {0}/5\n".format(attempts))
            attempts += 1
        if attempts == 6:
            sys.stdout.write("Could not upload {0}\n".format(input_file))
            sys.stdout.write("Exiting...\n")
            return None
        file_num += 1
    return job_id


def submit_custom_workflow(username, password, version, subject, subject_dir, options):
    """
    Submit a workflow to OSG for processing

    :param username: username to use when authenticating
    :param password: password to user when authenticating
    :param version: version of freesurfer to use
    :param subject:  name of subject in the file
    :param subject_dir:  path to file with FreeSurfer subject dir in a zip file
    :param options:  options to use when running workflow
    :return: job_id on success, None on error
    """

    timestamp, token = get_token(username, password)
    if options and not subject_dir:
        sys.stderr.write("You must provide a subject directory file if "
                         "using custom options!\n")
        sys.exit(1)
    query_params = {'userid': username,
                    'token': token,
                    'multicore': False,
                    'num_inputs': 1,
                    'options': options,
                    'version': version,
                    'subject': subject,
                    'timestamp': timestamp,
                    'jobname': "validation_{0}_{1}".format(subject, timestamp)}
    sys.stdout.write("Creating and submitting workflow\n")
    status, response = get_response(query_params, 'job', 'POST')
    if status != 200:
        response_obj = json.loads(response)
        error_message("Error while creating workflow:\n" +
                      response_obj['result'])
        sys.exit(1)
    response_obj = json.loads(response)
    job_id = response_obj['job_id']
    sys.stdout.write("Workflow {0} created\n".format(job_id))

    sys.stdout.write("Uploading input files\n")
    if subject_dir:
        attempts = 1
        sys.stdout.write("Uploading {0}\n".format(subject_dir))
        while attempts < 6:
            query_params = {'userid': username,
                            'timestamp': timestamp,
                            'token': token,
                            'jobid': job_id,
                            'filename': os.path.basename(subject_dir),
                            'subjectdir': True}
            input_path = os.path.abspath(subject_dir)
            if not os.path.isfile(input_path):
                sys.stderr.write("{0} is not present and is needed, "
                                 "exiting\n".format(input_path))
                sys.exit(1)
            with open(input_path, 'rb') as f:
                body = f.read()

            status, response = upload_item(query_params,
                                           'job/input',
                                           os.path.basename(subject_dir),
                                           body,
                                           'POST')
            if status == 200:
                sys.stdout.write("Uploaded {0} successfully\n".format(subject_dir))
                break
            response_obj = json.loads(response)
            sys.stdout.write("Error while uploading {0}\n".format(subject_dir))
            sys.stdout.write("Error: {0}\n".format(response_obj['result']))
            sys.stdout.write("Retrying upload, attempt {0}/5\n".format(attempts))
            attempts += 1
        if attempts == 6:
            sys.stdout.write("Could not upload {0}\n".format(subject_dir))
            sys.stdout.write("Exiting...\n")
            return None

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
    parser.add_argument('--subject', dest='subject',
                        help='subject name')
    parser.add_argument('--subject-dir', dest='subject_dir',
                        help='subject directory file')
    parser.add_argument('--input-file',
                        dest='input_files',
                        action='append',
                        default=[],
                        help='path to input file(s), this can be used '
                             'multiple times')
    parser.add_argument('--options',
                        dest='options',
                        default=None,
                        help='options to pass to FreeSurfer')
    parser.add_argument('--workflow', dest='workflow',
                        choices=['standard', 'multiple', 'custom'],
                        help='Type of workflow to run')
    parser.add_argument('--freesurfer-version',
                        dest='freesurfer_version',
                        choices=['5.3.0'],
                        default='5.3.0',
                        help='version of FreeSurfer to use')

    parser.add_argument('--dualcore', dest='multicore',
                        action='store_false', default=True,
                        help='Use 2 cores to process certain steps')
    # General arguments
    parser.add_argument('--user', dest='user', default=None,
                        help='Username to use to login')
    parser.add_argument('--password', dest='password',
                        default=None, help='Password used to login')

    args = parser.parse_args(sys.argv[1:])
    if not check_freesurfer():
        sys.stderr.write("FreeSurfer binaries not in path, exiting\n")
        sys.exit(1)

    if args.workflow in ('standard', 'multiple'):
        job_id = submit_standard_workflow(args.user,
                                          args.password,
                                          args.freesurfer_version,
                                          args.subject,
                                          args.input_files)
    elif args.workflow == 'custom':
        job_id = submit_custom_workflow(args.user,
                                        args.password,
                                        args.freesurfer_version,
                                        args.subject,
                                        args.subject_dir,
                                        args.options)
    else:
        sys.stderr.write("Invalid workflow type!\n")
        sys.exit(1)
    if job_id is None:
        sys.exit(1)
    running = True
    start_time = time.time()
    error = False
    while running:
        status = get_status(job_id, args.user, args.password)
        if (status != 'QUEUED') and (status != 'RUNNING'):
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

    if args.workflow == 'standard':
        compare_app = './compare_mri.py'
        if args.subject == 'MRN_3':
            reference_output = "../MRN_3_reference.tbz"
        elif args.subject == 'MRN_1':
            reference_output = "../MRN_1_reference.tbz"
    elif args.workflow == 'multiple':
        compare_app = './compare_mri.py'
        if args.subject == '175':
            reference_output = '../175_multiple_reference.tbz'
    elif args.workflow == 'custom':
        compare_app = './compare_recon1.py'
        if args.subject == 'MRN_1':
            reference_output = '../MRN_1_autorecon1_reference.tbz'
    else:
        sys.stderr.write("Invalid reference/workflow combination\n")
        sys.exit(1)
    try:
        subprocess.check_call([compare_app,
                               output_file,
                               reference_output])
    except subprocess.CalledProcessError:
        sys.exit(1)
    sys.exit(0)

if __name__ == '__main__':
    main()
