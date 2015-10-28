#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Licensed under the APL 2.0 license
import sys
import argparse
import os
import psycopg2


def process_image():
    """
    Process image specified from command line and

    :return: exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(description="Process uploaded MRI images")
    parser.add_argument('--image', dest='image', default=None, required=True,
                        help='Name of image file with MRI data (needs to be in mgz format)')
    parser.add_argument('--username', dest='username', default=None, required=True,
                        help='Username of submitter')
    parser.add_argument('--processing_directory', dest='processing_dir',
                        help='Directory files should go into for processing', required=True)
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])

    if not os.path.isfile(args.image):
        return 1

    psycopg2.





if __name__ == '__main__':
    sys.exit(process_image())
