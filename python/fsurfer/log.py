#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license

import log
import log.handlers
import os


LOG_FILENAME = '~/logs/fsurf.log'
MAX_BYTES = 1024*1024*50  # 50 MB
NUM_BACKUPS = 10  # 10 files


def initialize_logging():
    """
    Initialize logging for fsurf

    :return: None
    """
    logger = log.getLogger('fsurf')
    log_file = os.path.abspath(os.path.expanduser(LOG_FILENAME))
    handle = log.handlers.RotatingFileHandler(log_file,
                                              mode='a',
                                              maxBytes=MAX_BYTES,
                                              backupCount=NUM_BACKUPS)
    handle.setLevel(log.WARN)
    logger.addHandler(handle)


def set_debugging():
    """
    Configure logging to output debug messages

    :return: None
    """
    logger = log.getLogger('fsurf')
    log_file = os.path.abspath(os.path.expanduser('~/logs/fsurf_debug.log'))
    handle = log.FileHandler(log_file)
    handle.setLevel(log.DEBUG)
    logger.addHandler(handle)


def get_logger():
    """
    Get logger that can be used for logging

    :return: logger object
    """
    return log.getLogger('fsurf')