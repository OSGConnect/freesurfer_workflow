#!/usr/bin/env python
import argparse
import sys

import psycopg2

import fsurfer
import fsurfer.helpers

VERSION = fsurfer.__version__

PARAM_FILE_LOCATION = "/etc/freesurfer/db_info"
FREESURFER_BASE = '/local-scratch/fsurf/'


def update_completed_tasks(jobid):
    """
    Email user informing them that a workflow has completed

    :param jobid: id for workflow
    :return: None
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    try:
        conn = fsurfer.helpers.get_db_client()
        cursor = conn.cursor()
        logger.info("Incrementing tasks completd for workflow {0}".format(jobid))

        run_update = "UPDATE freesurfer_interface.job_run  " \
                     "SET tasks_completed = tasks_completed + 1 " \
                     "WHERE job_id = %s;"
        cursor.execute(run_update, [jobid])
        conn.commit()
        conn.close()
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return


def main():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Update fsurf job info and "
                                                 "email user about job "
                                                 "completion")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments identifying workflow
    parser.add_argument('--id', dest='workflow_id',
                        action='store', help='Pegasus workflow id to use')

    args = parser.parse_args(sys.argv[1:])
    update_completed_tasks(args.workflow_id)

    sys.exit(0)

if __name__ == '__main__':
    main()
