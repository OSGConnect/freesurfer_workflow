#!/usr/bin/env python

import json
import urlparse
import argparse
from wsgiref.simple_server import make_server
import socket
import sys
import random

TIMEZONE = "US/Central"


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


def publish_record(record, channel, redis_client):
    """
    Publishes a record to a Redis pub/sub channel

    :param record: dictionary representing record to publish
    :param redis_client: a redis client instance to use
    :return: None
    """
    if not redis_client or not channel:
        return
    redis_client.publish(channel, json.dumps(record))
    return


def delete_job(environ):
    """
    Remove a job from being processed
    TODO: placeholder for now

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    response_body = "{ \"status\": 200,\n \"result\": \"success\" }"
    status = '200 OK'
    if (random.random() > 0.9):
        # give an error in 10% of the cases
        return "{ \"status\": 500,\n \"result\": \"Server Error\" }", '500 Server Error'
    return response_body, status


def get_user_params(environ):
    """
    Get user id and security token from CGI query string

    :param environ: dictionary with environment variables (See PEP 333)
    :return: tuple with userid, security_token
    """
    query_dict = urlparse.parse_qs(environ['QUERY_STRING'])
    if 'userid' not in query_dict  or 'token' not in query_dict:
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
    userid, secret = get_user_params(environ)
    if not validate_user(userid, secret):
        response_body = "{ \"status\": 401,\n \"result\": \"invalid user\" }"
        status = '401 Not Authorized'
        return response_body, status

    response_body = "{ \n"
    response_body += " jobs: ["
    jobs = [(1, 'subj_1.mgz', 'job_name1', 'PROCESSING', 'http://test.url/output_1.mgz'),
            (23, 'subj_182.mgz', 'my_job2', 'COMPLETED', 'http://test.url/output_182.mgz'),]
    for job in jobs:
        response_body += '{ "id" : "{0}",'.format(job[0])
        response_body += ' "input" : "{0}"", "job_name": "{1}, "url": "{2}"}'.format(job[1], job[2], job[3])
        response_body += "},\n"
    if response_body[-2:-1] == ",\n":
        response_body = response_body[:-2] + "\n"
    response_body += "]\n}"
    status = '200 OK'
    return response_body, status


def submit_job(environ):
    """
    Submit a job to be processed
    TODO: placeholder for now

    :param environ: dictionary with environment variables (See PEP 333)
    :return: a tuple with response_body, status
    """
    status = '200 OK'
    if random.random() > 0.9:
        # give an error in 10% of the cases
        return "{ \"status\": 500,\n \"result\": \"server error\" }", '500 Server Error'

    return "{ \"status\": 200,\n \"result\": \"success\" }", '200 OK'

    return response_body, status


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
    srv = make_server(args.hostname, 8080, application)
    srv.serve_forever()
