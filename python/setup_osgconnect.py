#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurf on OSG Connect login
from distutils.core import setup
import fsurfer



setup(name='fsurf',
      version='1.3.15',
      description='Command line tool to submit freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      scripts=['fsurf', 'fsurf-config', 'email_fsurf_notification.py'],
      license='Apache 2.0')
