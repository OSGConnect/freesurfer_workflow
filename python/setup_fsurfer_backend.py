#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurf on OSG Connect login
from distutils.core import setup

setup(name='fsurfer-backend',
      version='0.6.5',
      description='Scripts to handle background freesurfer processing',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      scripts=['process_mri.py', 'update_fsurf_job.py'],
      license='Apache 2.0')

