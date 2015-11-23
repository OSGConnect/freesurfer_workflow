#!/usr/bin/env python

import argparse
import random
import socket
import sys
import urlparse
import json
from wsgiref.simple_server import make_server

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
               int(query_dict[key][0])
           except ValueError:
               return False
        elif val == bool:
           try:
               bool(query_dict[key][0])
           except ValueError:
               return False
    return True


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

    if random.random() > 0.9:
        # give an error in 10% of the cases
        response = {'status': 500,
                    'result': "Server Error"}
        return json.dumps(response), '500 Server Error'
    return json.dumps(response), status


def get_user_params(environ):
    """
    Get user id and security token from CGI query string

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    if 'userid' not in query_dict or 'token' not in query_dict:
        return '', ''
    user_id = query_dict['userid']
    token = query_dict['token']
    return user_id, token


def validate_user(userid, token):
    """
    Given an userid and security token, validate this against database

    :param userid: string with user id
    :param token:  security token
    :return: True if credentials are valid, false otherwise
    """
    import random
    if random.random() > 0.9:
        # give an error in 10% of the cases
        return False

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
                'jobs': [{'id': 1,
                          'input': 'subj_1.mgz',
                          'name': 'job_name1',
                          'status': 'PROCESSING',
                          'output': 'http://test.url/output_1.mgz'},
                         {'id': 23,
                          'input': 'subj_182.mgz',
                          'name': 'my_job2',
                          'status': 'COMPLETED',
                          'output': 'http://test.url/output_182.mgz'}]}

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
    if random.random() > 0.9:
        # give an error in 10% of the cases
        response = {'status': 500,
                    'result': "Server Error"}
        return json.dumps(response), '500 Server Error'
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

    if 'REQUEST_METHOD' not in environ:
        response_body = "No request method"
        response_headers = [('Content-Type', 'text/html'),
                            ('Content-Length', str(len(response_body)))]
        start_response('200 OK', response_headers)
        print response_body
        return [response_body]
    if environ['REQUEST_METHOD'] == 'GET':
        response_body, status = get_current_jobs(environ)
    elif environ['REQUEST_METHOD'] == 'POST':
        response_body, status = submit_job(environ)
    elif environ['REQUEST_METHOD'] == 'DELETE':
        response_body, status = delete_job(environ)
    else:
        response_body = '500 Server Error'
        status = '500 Server Error'

    response_headers = [('Content-Type', 'text/html'),
                        ('Content-Length', str(len(response_body)))]
    start_response(status, response_headers)
    print response_body
    return [response_body]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse request and act appropriately')
    parser.add_argument('--host', dest='hostname', default=socket.getfqdn(),
                        help='hostname of server')
    args = parser.parse_args(sys.argv[1:])
    srv = make_server(args.hostname, 8080, application)
    srv.serve_forever()
