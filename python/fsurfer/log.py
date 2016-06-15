#!/usr/bin/env python

# Copyright 2016 University of Chicago
# Licensed under the APL 2.0 license

import logging
import logging.handlers
import os


LOG_FILENAME = '~/logs/fsurf.log'
DEBUG_LOG_FILENAME = '~/logs/fsurf_debug.log'
LOG_FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s: %(message)s'
MAX_BYTES = 1024*1024*50  # 50 MB
NUM_BACKUPS = 10  # 10 files


def initialize_logging():
    """
    Initialize logging for fsurf

    :return: None
    """
    logger = logging.getLogger(__name__)
    log_file = os.path.abspath(os.path.expanduser(LOG_FILENAME))
    handle = logging.handlers.RotatingFileHandler(log_file,
                                                  mode='a',
                                                  maxBytes=MAX_BYTES,
                                                  backupCount=NUM_BACKUPS)
    handle.setLevel(logging.WARN)
    formatter = logging.Formatter(LOG_FORMAT)
    handle.setFormatter(formatter)
    logger.addHandler(handle)


def set_debugging():
    """
    Configure logging to output debug messages

    :return: None
    """
    logger = logging.getLogger(__name__)
    log_file = os.path.abspath(os.path.expanduser(DEBUG_LOG_FILENAME))
    handle = logging.handlers.RotatingFileHandler(log_file,
                                                  mode='a',
                                                  maxBytes=MAX_BYTES,
                                                  backupCount=NUM_BACKUPS)
    handle.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT)
    handle.setFormatter(formatter)
    logger.addHandler(handle)


def get_logger():
    """
    Get logger that can be used for logging

    :return: logger object
    """
    return logging.getLogger(__name__)
