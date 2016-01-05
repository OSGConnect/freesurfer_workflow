#!/usr/bin/env python

import argparse
import json
import socket
import sys
import urlparse
import hashlib
from wsgiref.simple_server import make_server

import psycopg2

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
TIMEZONE = "US/Central"


def validate_parameters(query_dict, parameters):
    """
    Check parameters in query_dict using the parameters specified
    :param query_dict: a dictionary with key / value pairs to test
    :param parameters: a dictionary with parameter name / type
                       specifying the type of parameters in the query_dict
    :return: true or false depending on whether the parameters are valid
    """

    for key, val in parameters:
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

    :param environ:
    :param file_name:
    :return:
    """
    uploaded_file = open(file_name, 'w')
    uploaded_file.write(environ['wsgi.input'].read())
    uploaded_file.close()


def get_db_parameters():
    """
    Read database parameters from a file and return it

    :return: a tuple of (database_name, user, password, hostname)
    """
    parameters = {}
    with open(PARAM_FILE_LOCATION) as param_file:
        line = param_file.readline()
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
    return psycopg2.connect(database=db, user=user, host=host)


def delete_job(environ):
    """
    Remove a job from being processed
    TODO: placeholder for now

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    status = '200 OK'
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'jobid': int}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'
    return json.dumps(response), status


def get_user_params(environ):
    """
    Get user id and security token from CGI query string

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    user_id = query_dict['userid']
    token = query_dict['token']
    return user_id, token


def get_user_salt(environ):
    """
    Get salt for a userid and return it

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token
    """
    status = '200 OK'
    userid, _ = get_user_params(environ)
    conn = get_db_client()
    cursor = conn.cursor()
    salt_query = "SELECT salt " \
                 "FROM users " \
                 "WHERE userid = %s;"

    try:
        cursor.execute(salt_query, userid)
        row = cursor.fetchone()
        if row:
            response = {'status': 200, 'result': row[0]}
        else:
            response = {'status': 400,
                        'result': 'Userid not found'}
            status = '400 Bad Request'
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        status = '500 Server Error'
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
                 "FROM users " \
                 "WHERE userid = %s;"
    try:
        cursor.execute(salt_query, userid)
        row = cursor.fetchone()
        if row:
            db_hash = hashlib.sha256(row[1] + str(timestamp)).hexdigest()
            return token == db_hash
        return False
    except Exception, e:
        conn.close()
        return False
    conn.close()
    return True


def get_current_jobs(environ):
    """
    Get status for all jobs submitted by user in last week
    TODO: placeholder for now

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'

    userid, secret = get_user_params(environ)
    if not validate_user(userid, secret):
        response = {'status': 401,
                    'result': "invalid user"}
        return json.dumps(response), '401 Not Authorized'

    response = {'status': 200,
                'jobs': []}
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, name, image_filename, state, job_date " \
                "FROM jobs " \
                "WHERE purged IS NOT TRUE AND age(job_date) < '7 days' AND userid = %s;"
    try:
        cursor.execute(job_query, userid)
        for row in cursor.fetchall():
            response['jobs'].append((row[0], row[1], row[2], row[3], row[4]))
    except Exception, e:
        response = {'status': 500,
                    'result': str(e)}
        return json.dumps(response), '500 Server Error'

    conn.close()
    status = '200 OK'
    return json.dumps(response), status


def submit_job(environ):
    """
    Submit a job to be processed
    TODO: placeholder for now

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    parameters = {'userid': str,
                  'token': str,
                  'filename': str,
                  'singlecore': bool,
                  'jobname': str}
    if not validate_parameters(query_dict, parameters):
        response = {'status': 400,
                    'result': "invalid or missing parameter"}
        return json.dumps(response), '400 Bad Request'

    response = {"status": 200,
                "result": "success"}
    return json.dumps(response), '200 OK'


def application(environ, start_response):
    """
    Get parameters from GET request and publish to redis channel

    :param environ: dictionary with environment variables (See PEP 333)
    :param start_response: callable function to handle responses (see PEP 333)
    :return: a list with the response_body to return to client
    """

    if environ['PATH_INFO'] == '/job':
        if environ['REQUEST_METHOD'] == 'GET':
            response_body, status = get_current_jobs(environ)
        elif environ['REQUEST_METHOD'] == 'POST':
            response_body, status = submit_job(environ)
        elif environ['REQUEST_METHOD'] == 'DELETE':
            response_body, status = delete_job(environ)
        else:
            response_body = "Invalid request"
            status = "400 Bad Request"
    elif environ['PATH_INFO'] == '/job/output':
        response_body, status = get_job_output(environ)
    elif environ['PATH_INFO'] == '/userid/salt':
        response_body, status = get_user_salt(environ)
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
    srv = make_server(args.hostname, args.db_param_file, 8080, application)
    srv.serve_forever()
