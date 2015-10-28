#!/usr/bin/env python
import argparse
import sys

def generate_dax():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    errors = False
    parser = argparse.ArgumentParser(description="generate a pegasus workflow")
    parser.add_argument('--Sub', dest='subject', default=None, required=True,
                        help='Subject id(s) to process (e.g. --Sub 182,64,43)')
    parser.add_argument('--SkipRecon', dest='skip_recon',
                        action='store_true',
                        help='Skip recon processing')
    parser.add_argument('--multi_core', dest='multicore',
                        action='store_true',
                        help='Do all processing in a single job')
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    args = parser.parse_args(sys.argv[1:])
