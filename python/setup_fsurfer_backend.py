#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurf on OSG Connect login
from distutils.core import setup
import fsurfer

setup(name='fsurfer-backend',
      version=fsurfer.__version__,
      description='Scripts to handle background freesurfer processing',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      scripts=['process_mri.py',
               'update_fsurf_job.py',
               'purge_inputs.py',
               'purge_results.py',
               'warn_purge.py',
               'delete_jobs.py',
               'fsurf_user_admin.py'],
      license='Apache 2.0')

