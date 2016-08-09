#!/usr/bin/env python

import argparse
import socket
import sys
import hashlib
import os
import tempfile

import psycopg2
from flask import Flask
import flask

CONFIG_FILE_LOCATION = "/etc/fsurf/fsurf-prod.config"
FREESURFER_BASE = '/stash2/user/fsurf/'
TIMEZONE = "US/Central"
URL_PREFIX = "/freesurfer"

app = Flask(__name__)


def validate_parameters(parameters):
    """
    Check parameters in request using the parameters specified
    :param parameters: a dictionary with parameter name / type
                       specifying the type of parameters in the request
    :return: true or false depending on whether the parameters are valid
    """
    for key, val in parameters.iteritems():
        if key not in flask.request.args:
            return False
        if val == int:
            try:
                int(flask.request.args[key])
            except ValueError:
                return False
        elif val == bool:
            if flask.request.args[key].lower() not in ('true', 'false'):
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
    # with open(CONFIG_FILE_LOCATION) as param_file:
    #     for line in param_file:
    #         key, val = line.strip().split('=')
    #         parameters[key.strip()] = val.strip()
    parameters['database'] = app.config['DB_NAME']
    parameters['user'] = app.config['DB_USER']
    parameters['password'] = app.config['DB_PASSWD']
    parameters['hostname'] = app.config['DB_HOST']
    return (parameters['database'],
            parameters['user'],
            parameters['password'],
            parameters['hostname'])


def flask_error_response(status, message):
    """
    Generate and return a flask error response

    :param status:   integer status code for response
    :param message:  string with response message
    :return:         Flask response with appropriate status
                     and message
    """
    response = flask.jsonify({'status': status,
                              'result': message.lower()})
    response.status_code = status
    response.status = message
    return response


def get_db_client():
    """
    Get a postgresql client instance and return it

    :return: a redis client instance or None if failure occurs
    """
    db, user, password, host = get_db_parameters()
    return psycopg2.connect(database=db, user=user, host=host, password=password)


@app.route(URL_PREFIX + '/job', methods=['DELETE'])
def delete_job():
    """
    Remove a job from being processed

    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")

    userid, token, timestamp = get_user_params()
    if not validate_user(userid, token, timestamp):
        return flask_error_response(401, "Invalid username or password")
    job_id = flask.request.args['jobid']
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT state FROM freesurfer_interface.jobs  " \
                "WHERE id = %s;"
    try:
        cursor.execute(job_query, [job_id])
        if cursor.rowcount != 1:
            return flask_error_response(400, "Job not found")
        row = cursor.fetchone()
        if row:
            state = row[0]
        else:
            state = 'None'
        if state not in ['PROCESSING', 'UPLOADED', 'FAILED', 'COMPLETED']:
            return flask_error_response(400,
                                        "Workflow has already been marked for "
                                        "deletion or has been deleted")
        else:
            job_update = "UPDATE freesurfer_interface.jobs  " \
                         "SET state = 'DELETE PENDING'" \
                         "WHERE id = %s;"
            cursor.execute(job_update, [job_id])
            if cursor.rowcount != 1:
                conn.rollback()
                return flask_error_response(400, "Job not found")
            conn.commit()

    except Exception as e:
        conn.rollback()
        return flask_error_response(500, str(e))
    finally:
        conn.close()
    return flask.jsonify(response)


def get_user_params():
    """
    Get user id and security token from CGI query string

    :return: tuple with userid, security_token, timestamp
    """
    if 'userid' in flask.request.args:
        user_id = flask.request.args['userid']
    else:
        user_id = None
    if 'token' in flask.request.args:
        token = flask.request.args['token']
    else:
        token = None
    if 'timestamp' in flask.request.args:
        timestamp = flask.request.args['timestamp']
    else:
        timestamp = None
    return user_id, token, timestamp


@app.route(URL_PREFIX + '/user/salt')
def get_user_salt():
    """
    Get salt for a userid and return it

    :return: a tuple with response_body, status
    """
    userid, _, _ = get_user_params()
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
            return flask_error_response(401, 'User account disabled')
        else:
            return flask_error_response(400, 'Userid not found')
    except Exception as e:
        return flask_error_response(500,
                                    '500 Server Error\n'
                                    'Exception: {0}'.format(e))
    finally:
        conn.commit()
        conn.close()
    return flask.jsonify(response)


@app.route(URL_PREFIX + '/user/password', methods=['PUT'])
def set_user_password():
    """
    Set password for a userid

    :return: a tuple with response_body, status
    """
    parameters = {'userid': str,
                  'salt': str,
                  'token': str,
                  'pw_hash': str}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")

    userid, token, timestamp = get_user_params()
    if not validate_user(userid, token, timestamp):
        return flask_error_response(401, "Invalid username or password")
    conn = get_db_client()
    cursor = conn.cursor()
    user_update = "UPDATE freesurfer_interface.users " \
                  "SET salt = %s, password = %s " \
                  "WHERE username = %s;"
    try:
        cursor.execute(user_update, (flask.request.args['salt'],
                                     flask.request.args['pw_hash'],
                                     userid))
        if cursor.rowcount == 1:
            response = {'status': 200,
                        'result': 'Password updated'}
        elif cursor.rowcount == 0:
            return flask_error_response(404, 'Userid not found')
        else:
            return flask_error_response(400, 'Error: ' + cursor.statusmessage)
    except Exception as e:
        return flask_error_response(500,
                                    '500 Server Error\n'
                                    'Exception: {0}'.format(e))
    finally:
        conn.commit()
        conn.close()
    return flask.jsonify(response)


@app.route(URL_PREFIX + '/user/validate')
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


@app.route(URL_PREFIX + '/job', methods=['GET'])
def get_current_jobs():
    """
    Get status for all jobs submitted by user in last week

    :return: a tuple with response_body, status
    """
    parameters = {'userid': str,
                  'token': str,
                  'all': bool}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")
    userid, secret, timestamp = get_user_params()
    if not validate_user(userid, secret, timestamp):
        return flask_error_response(401, "Invalid username or password")
    response = {'status': 200,
                'jobs': []}
    conn = get_db_client()
    cursor = conn.cursor()
    if flask.request.args['all'].lower() == 'true':
        job_query = "SELECT id, " \
                    "       subject, " \
                    "       state, " \
                    "       date_trunc('seconds', job_date), " \
                    "       multicore " \
                    "FROM freesurfer_interface.jobs " \
                    "WHERE purged IS NOT TRUE AND " \
                    "      username = %s " \
                    "ORDER BY job_date DESC;"
    else:
        job_query = "SELECT id, " \
                    "       subject, " \
                    "       state, " \
                    "       date_trunc('seconds', job_date), " \
                    "       multicore " \
                    "FROM freesurfer_interface.jobs " \
                    "WHERE purged IS NOT TRUE AND " \
                    "      age(job_date) < '1 month' AND username = %s " \
                    "ORDER BY job_date DESC;"
    try:
        cursor.execute(job_query, [userid])
        for row in cursor.fetchall():
            response['jobs'].append((row[0],
                                     row[1],
                                     row[2],
                                     row[3].strftime("%Y-%m-%d %H:%M:%S"),
                                     row[4]))
    except Exception, e:
        return flask_error_response(500,
                                    "500 Server Error\n"
                                    "Exception: {0}".format(e))
    finally:
        conn.commit()
        conn.close()

    return flask.jsonify(response)


@app.route(URL_PREFIX + '/job/status')
def get_job_status():
    """
    Get status for job specified

    :return: a tuple with response_body, status
    """
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")
    userid, secret, timestamp = get_user_params()
    if not validate_user(userid, secret, timestamp):
        return flask_error_response(401, "Invalid username or password")
    response = {'status': 200,
                'job': []}
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT state " \
                "FROM freesurfer_interface.jobs " \
                "WHERE id = %s AND username = %s;"
    try:
        cursor.execute(job_query, [flask.request.args['jobid'], userid])
        row = cursor.fetchone()
        if row is None:
            return flask_error_response(404, "Invalid workflow id")
        else:
            response['job_status'] = row[0]
    except Exception as e:
        return flask_error_response(500,
                                    "500 Server Error\n"
                                    "Exception: {0}".format(e))
    finally:
        conn.commit()
        conn.close()
    return flask.jsonify(response)


@app.route(URL_PREFIX + '/job/input', methods=['POST'])
def get_input():
    """
    Submit an input for a job to be processed

    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    parameters = {'userid': str,
                  'token': str,
                  'filename': str,
                  'subjectdir': bool,
                  'jobid': int}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")
    userid, token, timestamp = get_user_params()
    if not validate_user(userid, token, timestamp):
        return flask_error_response(401, "Invalid username or password")
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
    conn = get_db_client()
    cursor = conn.cursor()
    input_insert = "INSERT INTO freesurfer_interface.input_files(filename," \
                   "                                             path," \
                   "                                             job_id," \
                   "                                             subject_dir)" \
                   "VALUES(%s, %s, %s, %s)"
    try:
        temp_dir = tempfile.mkdtemp(dir=output_dir)
        input_file = os.path.join(temp_dir,
                                  flask.request.args['filename'])
        fh = flask.request.files['file']
        fh.save(input_file)
        cursor.execute(input_insert,
                       [flask.request.args['filename'],
                        input_file,
                        flask.request.args['jobid'],
                        flask.request.args['subjectdir']])

        conn.commit()
    except Exception, e:
        conn.rollback()
        return flask_error_response(500,
                                    "500 Server Error\n"
                                    "Exception: {0}".format(e))
    finally:
        conn.close()
    return flask.jsonify(response)


@app.route(URL_PREFIX + '/job', methods=['POST'])
def submit_job():
    """
    Submit a job to be processed

    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    parameters = {'userid': str,
                  'token': str,
                  'multicore': bool,
                  'num_inputs':  int,
                  'options': str,
                  'version': str,
                  'subject': str,
                  'jobname': str}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")
    userid, token, timestamp = get_user_params()
    if not validate_user(userid, token, timestamp):
        return flask_error_response(401, "Invalid username or password")
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
    conn = get_db_client()
    cursor = conn.cursor()
    job_insert = "INSERT INTO freesurfer_interface.jobs(name," \
                 "                                      state," \
                 "                                      multicore," \
                 "                                      username," \
                 "                                      num_inputs," \
                 "                                      options," \
                 "                                      version," \
                 "                                      subject)" \
                 "VALUES(%s, %s, 'UPLOADED', %s, %s, %s)" \
                 "RETURNING id"
    try:
        cursor.execute(job_insert,
                       [flask.request.args['jobname'],
                        flask.request.args['multicore'],
                        userid,
                        flask.request.args['num_inputs'],
                        flask.request.args['options'],
                        flask.request.args['version'],
                        flask.request.args['subject']])
        job_id = cursor.fetchone()[0]
        response['job_id'] = job_id
        conn.commit()
    except Exception, e:
        conn.rollback()
        return flask_error_response(500,
                                    "500 Server Error\n"
                                    "Exception: {0}".format(e))
    finally:
        conn.close()
    return flask.jsonify(response)


@app.route(URL_PREFIX + '/job/output')
def get_job_output():
    """
    Return the output from a job

    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")
    userid, token, timestamp = get_user_params()
    if not validate_user(userid, token, timestamp):
        return flask_error_response(401, "Invalid username or password")
    output_dir = os.path.join(FREESURFER_BASE, userid, 'results')
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, subject, state " \
                "FROM freesurfer_interface.jobs " \
                "WHERE id = %s AND username = %s;"
    try:
        cursor.execute(job_query, [flask.request.args['jobid'], userid])
        row = cursor.fetchone()
        if cursor.rowcount == 0:
            return flask_error_response(404,
                                        "Workflow not found")
        elif row[2] != 'COMPLETED':
            return flask_error_response(404,
                                        "Workflow does not have any output to "
                                        "download")

        else:
            output_filename = os.path.join(output_dir,
                                           "{0}_{1}_output.tar.bz2".format(row[0],
                                                                           row[1]))
            if os.path.isfile(output_filename):
                filename = str(os.path.basename(output_filename))
                return flask.send_file(output_filename,
                                       mimetype="application/octet-stream",
                                       as_attachment=True,
                                       attachment_filename=filename)
    except Exception, e:
        return flask_error_response(500,
                                    "500 Server Error\n"
                                    "Exception: {0}".format(e))
    finally:
        conn.commit()
        conn.close()
    return flask.jsonify(response)


@app.route(URL_PREFIX + '/job/log')
def get_job_log():
    """
    Return the logs from a job

    :return: a tuple with response_body, status
    """
    response = {"status": 200,
                "result": "success"}
    parameters = {'userid': str,
                  'token': str,
                  'jobid': str}
    if not validate_parameters(parameters):
        return flask_error_response(400, "Invalid or missing parameter")
    userid, token, timestamp = get_user_params()
    if not validate_user(userid, token, timestamp):
        return flask_error_response(401, "Invalid username or password")
    output_dir = os.path.join(FREESURFER_BASE, userid, 'results')
    conn = get_db_client()
    cursor = conn.cursor()
    job_query = "SELECT id, subject, state " \
                "FROM freesurfer_interface.jobs " \
                "WHERE id = %s AND username = %s;"
    try:
        cursor.execute(job_query, [flask.request.args['jobid'], userid])
        row = cursor.fetchone()
        if cursor.rowcount == 0:
            return flask_error_response(404,
                                        "Workflow not found")
        elif row[2] != 'COMPLETED':
            return flask_error_response(404,
                                        "Workflow does not have any logs to "
                                        "download")
        else:
            output_filename = os.path.join(output_dir,
                                           "recon_all-{0}.log".format(row[0]))
            if os.path.isfile(output_filename):
                filename = str(os.path.basename(output_filename))
                return flask.send_file(output_filename,
                                       mimetype="application/octet-stream",
                                       as_attachment=True,
                                       attachment_filename=filename)
    except Exception as e:
        return flask_error_response(500,
                                    "500 Server Error\n"
                                    "Exception: {0}".format(e))
    finally:
        conn.commit()
        conn.close()
    return flask.jsonify(response)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse request and act appropriately')
    parser.add_argument('--host', dest='hostname', default=socket.getfqdn(),
                        help='hostname of server')
    parser.add_argument('--port', dest='port', default=5000, type=int,
                        help='hostname of server')
    parser.add_argument('--config', dest='config_file', default=CONFIG_FILE_LOCATION,
                        help='location of file with configuration')
    parser.add_argument('--debug', dest='debug',
                        action='store_true', default=False,
                        help='Output debug messages')
    args = parser.parse_args(sys.argv[1:])

    if 'FSURF_CONFIG_FILE' in os.environ and os.environ['FSURF_CONFIG_FILE']:
        app.config.from_envvar('FSURF_CONFIG_FILE')
    else:
        app.config.from_pyfile(args.config_file)
    if app.config['TESTING']:
        global URL_PREFIX
        URL_PREFIX += "_testing"
    app.run(args.hostname, args.port, args.debug)
