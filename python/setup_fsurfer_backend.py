#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurf on OSG Connect login
from distutils.core import setup

setup(name='fsurfer-backend',
      version='PKG_VERSION',
      description='Scripts to handle background freesurfer processing',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      scripts=['process_mri.py',
               'workflow_completed.py',
               'purge_inputs.py',
               'purge_results.py',
               'warn_purge.py',
               'delete_jobs.py',
               'task_completed.py',
               'resync_workflows.py',
               'fsurf_user_admin.py'],
      license='Apache 2.0')

