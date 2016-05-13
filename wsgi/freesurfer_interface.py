#!/usr/bin/env python

import argparse
import json
import socket
import sys
import urlparse
import hashlib
import os
import tempfile
from wsgiref.simple_server import make_server

import psycopg2

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/stash2/user/fsurf/'
TIMEZONE = "US/Central"


def validate_parameters(query_dict, parameters):
    """
    Check parameters in query_dict using the parameters specified
    :param query_dict: a dictionary with key / value pairs to test
    :param parameters: a dictionary with parameter name / type
                       specifying the type of parameters in the query_dict
    :return: true or false depending on whether the parameters are valid
    """
    for key, val in parameters.iteritems():
        if key not in query_dict:
            return False
        if val == int:
            try:
                int(query_dict[key])
            except ValueError:
                return False
        elif val == bool:
            try:
                bool(query_dict[key])
            except ValueError:
                return False
    return True


def save_file(environ, file_name):
    """
    Save a file that's uploaded using POST

    :param environ: wsgi environment dictionary
    :param file_name: name of file to save to
    :return: nothing
    """
    uploaded_file = open(file_name, 'wb')
    uploaded_file.write(environ['wsgi.input'].read())
    uploaded_file.close()


def get_db_parameters():
    """
    Read database parameters from a file and return it

    :return: a tuple of (database_name, user, password, hostname)
    """
    parameters = {}
    with open(PARAM_FILE_LOCATION) as param_file:
        for line in param_file:
            key, val = line.strip().split('=')
            parameters[key.strip()] = val.strip()
    return (parameters['database'],
            parameters['user'],
            parameters['password'],
            parameters['hostname'])


def get_db_client():
    """
    Get a postgresql client instance and return it

    :return: a redis client instance or None if failure occurs
    """
    db, user, password, host = get_db_parameters()
    return psycopg2.connect(database=db, user=user, host=host, password=password)


def delete_job(environ):
    """
    Remove a job from being processed

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    status = '200 OK'
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'

    userid, token, timestamp = get_user_params(environ)
    if not validate_user(userid, token, timestamp):
        response = {'status': 401,
                    'result': "invalid user/password"}
        return json.dumps(response), '401 Not Authorized'
    job_id = query_dict['jobid'][0]
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT state FROM freesurfer_interface.jobs  " \
                "WHERE id = %s;"
    try:
        cursor.execute(job_query, [job_id])
        if cursor.rowcount != 1:
            response = {'status': 400,
                        'result': 'Job not found'}
            status = '400 Bad Request'
        row = cursor.fetchone()
        if row:
            state = row[0]
        else:
            state = 'None'
        if state not in ['PROCESSING', 'UPLOADED', 'FAILED', 'COMPLETED']:
            response = {'status': 400,
                        'result': 'Job has already been marked for deletion '
                                  'or has been deleted'}
            status = '400 Bad Request'
        else:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'DELETE PENDING'" \
                         "WHERE id = %s;"
            cursor.execute(job_update, [job_id])
            if cursor.rowcount != 1:
                response = {'status': 400,
                            'result': 'Job not found'}
                status = '400 Bad Request'
            conn.commit()

    except Exception, e:
        conn.rollback()
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.close()
    return json.dumps(response), status


def get_user_params(environ):
    """
    Get user id and security token from CGI query string

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token, timestamp
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    if 'userid' in query_dict:
        user_id = query_dict['userid'][0]
    else:
        user_id = None
    if 'token' in query_dict:
        token = query_dict['token'][0]
    else:
        token = None
    if 'timestamp' in query_dict:
        timestamp = query_dict['timestamp'][0]
    else:
        timestamp = None
    return user_id, token, timestamp


def get_user_salt(environ):
    """
    Get salt for a userid and return it

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token
    """
    status = '200 OK'
    userid, _, _ = get_user_params(environ)
    conn = get_db_client()
    cursor = conn.cursor()
    salt_query = "SELECT salt " \
                 "FROM freesurfer_interface.users " \
                 "WHERE username = %s;"

    try:
        cursor.execute(salt_query, [userid])
        row = cursor.fetchone()
        if row and not row[0].startswith('xxx'):
            response = {'status': 200, 'result': row[0]}
        elif row and row[0].startswith('xxx'):
            response = {'status': 401,
                        'result': 'User account disabled'}
            status = '401 Not Authorized'
        else:
            response = {'status': 400,
                        'result': 'Userid not found'}
            status = '400 Bad Request'
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.commit()
        conn.close()
    return json.dumps(response), status


def set_user_password(environ):
    """
    Set password for a userid

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token
    """
    status = '200 OK'
    userid, _, _ = get_user_params(environ)
    conn = get_db_client()
    cursor = conn.cursor()
    salt_query = "UPDATE freesurfer_interface.users " \
                 "SET salt = %s, password = %s " \
                 "WHERE username = %s;"
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    try:
        cursor.execute(salt_query, (query_dict['salt'],
                                    query_dict['password'],
                                    userid))
        if cursor.rowcount == 1:
            response = {'status': 200,
                        'result': 'Password updated'}
        elif cursor.rowcount == 0:
            response = {'status': 400,
                        'result': 'Userid not found'}
        else:
            response = {'status': 400,
                        'result': 'Error: ' + cursor.statusmessage}
            status = '400 Bad Request'
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.commit()
        conn.close()
    return json.dumps(response), status


def validate_user(userid, token, timestamp):
    """
    Given an userid and security token, validate this against database

    :param userid: string with user id
    :param token:  security token
    :param timestamp: string with the unix timestamp of when token was made
    :return: True if credentials are valid, False otherwise
    """
    conn = get_db_client()
    cursor = conn.cursor()
    salt_query = "SELECT salt, password " \
                 "FROM freesurfer_interface.users " \
                 "WHERE username = %s;"
    try:
        cursor.execute(salt_query, [userid])
        row = cursor.fetchone()
        if row:
            db_hash = hashlib.sha256(row[1] + str(timestamp)).hexdigest()
            return token == db_hash
        return False
    except psycopg2.Error:
        return False
    finally:
        conn.commit()
        conn.close()


def get_current_jobs(environ):
    """
    Get status for all jobs submitted by user in last week

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'all': bool}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'

    userid, secret, timestamp = get_user_params(environ)
    if not validate_user(userid, secret, timestamp):
        response = {'status': 401,
                    'result': "invalid user/password"}
        return json.dumps(response), '401 Not Authorized'

    response = {'status': 200,
                'jobs': []}
    status = '200 OK'
    conn = get_db_client()
    cursor = conn.cursor()
    if bool(query_dict['all'][0]):
        job_query = "SELECT id, subject, state, job_date, multicore " \
                    "FROM freesurfer_interface.jobs " \
                    "WHERE purged IS NOT TRUE AND " \
                    "      username = %s " \
                    "ORDER BY job_date DESC;"
    else:
        job_query = "SELECT id, subject, state, job_date, multicore " \
                    "FROM freesurfer_interface.jobs " \
                    "WHERE purged IS NOT TRUE AND " \
                    "      age(job_date) < '7 days' AND username = %s " \
                    "ORDER BY job_date DESC;"
    try:
        cursor.execute(job_query, [userid])
        for row in cursor.fetchall():
            response['jobs'].append((row[0], row[1], row[2],
                                     row[3].isoformat(),
                                     row[4]))
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.commit()
        conn.close()

    return json.dumps(response), status


def get_job_status(environ):
    """
    Get status for job specified

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'

    userid, secret, timestamp = get_user_params(environ)
    if not validate_user(userid, secret, timestamp):
        response = {'status': 401,
                    'result': "invalid user/password"}
        return json.dumps(response), '401 Not Authorized'

    response = {'status': 200,
                'job': []}
    status = '200 OK'
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT state " \
                "FROM freesurfer_interface.jobs " \
                "WHERE id = %s AND username = %s;"
    try:
        cursor.execute(job_query, [query_dict['jobid'][0], userid])
        row = cursor.fetchone()
        if row is None:
            response = {'status': 404,
                        'result': "invalid workflow id"}
            status = '404 Not Found'
        else:
            response['job_status'] = row[0]
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.commit()
        conn.close()
    return json.dumps(response), status


def submit_job(environ):
    """
    Submit a job to be processed

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    status = '200 OK'
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'filename': str,
                  'multicore': bool,
                  'subject': str,
                  'jobname': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'
    userid, token, timestamp = get_user_params(environ)
    if not validate_user(userid, token, timestamp):
        response = {'status': 401,
                    'result': "invalid user/password"}
        return json.dumps(response), '401 Not Authorized'
    # setup user directories if not present
    user_dir = os.path.join(FREESURFER_BASE, userid)
    if not os.path.exists(user_dir):
        os.mkdir(user_dir, 0o770)
    output_dir = os.path.join(user_dir, 'input')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir, 0o770)
    if not os.path.exists(os.path.join(user_dir, 'results')):
        os.mkdir(os.path.join(user_dir, 'results'), 0o770)
    if not os.path.exists(os.path.join(user_dir, 'output')):
        os.mkdir(os.path.join(user_dir, 'output'), 0o770)
    if not os.path.exists(os.path.join(user_dir, 'workflows')):
        os.mkdir(os.path.join(user_dir, 'workflows'), 0o770)
    # upload
    temp_dir = tempfile.mkdtemp(dir=output_dir)
    input_file = os.path.join(temp_dir,
                              "{0}_defaced.mgz".format(query_dict['subject'][0]))
    save_file(environ, input_file)
    conn = get_db_client()
    cursor = conn.cursor()
    job_insert = "INSERT INTO freesurfer_interface.jobs(name," \
                 "                                      image_filename," \
                 "                                      state," \
                 "                                      multicore," \
                 "                                      username," \
                 "                                      subject)" \
                 "VALUES(%s, %s, 'UPLOADED', %s, %s, %s)" \
                 "RETURNING id"
    try:
        cursor.execute(job_insert,
                       [query_dict['jobname'][0],
                        input_file,
                        query_dict['multicore'][0],
                        userid,
                        query_dict['subject'][0]])
        job_id = cursor.fetchone()[0]
        response['job_id'] = job_id
        conn.commit()
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
        conn.rollback()
    finally:
        conn.close()
    return json.dumps(response), status


def get_job_output(environ):
    """
    Return the output from a job

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    status = '200 OK'
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'
    userid, token, timestamp = get_user_params(environ)
    if not validate_user(userid, token, timestamp):
        response = {'status': 401,
                    'result': "invalid user/password"}
        return json.dumps(response), '401 Not Authorized'
    output_dir = os.path.join(FREESURFER_BASE, userid, 'results')
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, subject, state " \
                "FROM freesurfer_interface.jobs " \
                "WHERE id = %s AND username = %s;"
    try:
        cursor.execute(job_query, [query_dict['jobid'][0], userid])
        row = cursor.fetchone()
        if cursor.rowcount == 0:
            response['status'] = 404
            response["result"] = "Workflow not found"
            status = "404 Not Found"
        elif row[2] != 'COMPLETED':
            response['status'] = 404
            response["result"] = "Workflow does not have any output to download"
            status = "404 Not Found"
        else:
            output_filename = os.path.join(output_dir,
                                           "{0}_{1}_output.tar.bz2".format(row[0],
                                                                           row[1]))
            if os.path.isfile(output_filename):
                response['result'] = "Output found"
                response['filename'] = output_filename
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.commit()
        conn.close()
    return json.dumps(response), status


def get_job_log(environ):
    """
    Return the logs from a job

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    status = '200 OK'
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'
    userid, token, timestamp = get_user_params(environ)
    if not validate_user(userid, token, timestamp):
        response = {'status': 401,
                    'result': "invalid user/password"}
        return json.dumps(response), '401 Not Authorized'
    output_dir = os.path.join(FREESURFER_BASE, userid, 'results')
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, subject, state " \
                "FROM freesurfer_interface.jobs " \
                "WHERE id = %s AND username = %s;"
    try:
        cursor.execute(job_query, [query_dict['jobid'][0], userid])
        row = cursor.fetchone()
        if cursor.rowcount == 0:
            response['status'] = 404
            response["result"] = "Workflow not found"
            status = "404 Not Found"
        elif row[2] != 'COMPLETED':
            response['status'] = 404
            response["result"] = "Workflow does not have any logs to download"
            status = "404 Not Found"
        else:
            output_filename = os.path.join(output_dir,
                                           "recon_all-{0}.log".format(row[0]))
            if os.path.isfile(output_filename):
                response['result'] = "Output found"
                response['filename'] = output_filename
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
    finally:
        conn.commit()
        conn.close()
    return json.dumps(response), status


def application(environ, start_response):
    """
    Get parameters from GET request and publish to redis channel

    :param environ: dictionary with environment variables (See PEP 333)
    :param start_response: callable function to handle responses (see PEP 333)
    :return: a list with the response_body to return to client
    """

    if environ['PATH_INFO'] == '/freesurfer/job':
        if environ['REQUEST_METHOD'] == 'GET':
            response_body, status = get_current_jobs(environ)
        elif environ['REQUEST_METHOD'] == 'POST':
            response_body, status = submit_job(environ)
        elif environ['REQUEST_METHOD'] == 'DELETE':
            response_body, status = delete_job(environ)
        else:
            response_body = "Invalid request"
            status = "400 Bad Request"
    elif environ['PATH_INFO'] == '/freesurfer/job/output':
        # need to do something a bit special because
        # we're returning a file
        response_body, status = get_job_output(environ)
        response_obj = json.loads(response_body)
        if response_obj['result'] == 'Output found':
            filename = response_obj['filename']
            response_headers = [('Content-Type', 'application/x-bzip2'),
                                ('Content-length', str(os.path.getsize(filename))),
                                ('Content-Disposition',
                                 'attachment; filename=' + str(os.path.basename(filename)))]
            try:
                fh = open(filename, 'rb')
                start_response(status, response_headers)
                if 'wsgi.file_wrapper' in environ:
                    return environ['wsgi.file_wrapper'](fh, 4096)
                else:
                    return iter(lambda: fh.read(4096), '')
            except IOError:
                response_body = json.dumps({'status': 500,
                                            'result': 'Could not read output file'})
                status = '500 Server Error'
    elif environ['PATH_INFO'] == '/freesurfer/job/status':
        response_body, status = get_job_status(environ)
    elif environ['PATH_INFO'] == '/freesurfer/job/log':
        # need to do something a bit special because
        # we're returning a file
        response_body, status = get_job_log(environ)
        response_obj = json.loads(response_body)
        if response_obj['result'] == 'Output found':
            filename = response_obj['filename']
            response_headers = [('Content-Type', 'text/plain'),
                                ('Content-length', str(os.path.getsize(filename))),
                                ('Content-Disposition',
                                 'attachment; filename=' + str(os.path.basename(filename)))]
            try:
                fh = open(filename, 'rb')
                start_response(status, response_headers)
                if 'wsgi.file_wrapper' in environ:
                    return environ['wsgi.file_wrapper'](fh, 4096)
                else:
                    return iter(lambda: fh.read(4096), '')
            except IOError:
                response_body = json.dumps({'status': 500,
                                            'result': 'Could not read output file'})
                status = '500 Server Error'
    elif environ['PATH_INFO'] == '/freesurfer/user/salt':
        response_body, status = get_user_salt(environ)
    elif environ['PATH_INFO'] == '/freesurfer/user/password':
        response_body, status = set_user_password(environ)
    else:
        status = '400 Bad Request'
        response_body = json.dumps({'status': 400,
                                    'result': 'Bad action'})
    response_headers = [('Content-Type', 'text/html'),
                        ('Content-Length', str(len(response_body)))]
    start_response(status, response_headers)
    print response_body
    return [response_body]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse request and act appropriately')
    parser.add_argument('--host', dest='hostname', default=socket.getfqdn(),
                        help='hostname of server')
    parser.add_argument('--dbparams', dest='db_param_file', default=PARAM_FILE_LOCATION,
                        help='location of file with database information')
    args = parser.parse_args(sys.argv[1:])
    if args.db_param_file != PARAM_FILE_LOCATION:
        PARAM_FILE_LOCATION = args.db_param_file
    srv = make_server(args.hostname, 8080, application)
    srv.serve_forever()
