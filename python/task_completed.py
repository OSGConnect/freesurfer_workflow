#!/usr/bin/env python
import argparse
import sys

import psycopg2

import fsurfer
import fsurfer.helpers

VERSION = fsurfer.__version__


def update_completed_tasks(job_run_id):
    """
    Increment # of tasks completed for a workflow

    :param job_run_id: id for job run entry
    :return: None
    """
    fsurfer.log.initialize_logging()
    logger = fsurfer.log.get_logger()

    try:
        conn = fsurfer.helpers.get_db_client()
        cursor = conn.cursor()
        logger.info("Incrementing tasks completed for workflow {0}".format(job_run_id))

        run_update = "UPDATE freesurfer_interface.job_run " \
                     "SET tasks_completed = tasks_completed + 1 " \
                     "WHERE id = %s AND" \
                     "      tasks_completed < tasks "
        logger.info("Updating run {0}".format(job_run_id))
        cursor.execute(run_update, [job_run_id])
        conn.commit()
        conn.close()
    except psycopg2.Error as e:
        logger.exception("Got pgsql error: {0}".format(e))
        return


def main():
    """
    Increment the number of jobs completed for a fsurf workflow

    :return: True if any errors occurred during DAX generaton
    """
    parser = argparse.ArgumentParser(description="Update fsurf job run entry "
                                                 "when a task completes")
    # version info
    parser.add_argument('--version', action='version', version='%(prog)s ' + VERSION)
    # Arguments identifying workflow
    parser.add_argument('--id', dest='workflow_id',
                        action='store', help='Workflow id for job being incremented')

    args = parser.parse_args(sys.argv[1:])
    update_completed_tasks(args.workflow_id)

    sys.exit(0)

if __name__ == '__main__':
    main()
