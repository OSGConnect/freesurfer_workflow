#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurf on OSG Connect login
from distutils.core import setup

from fsurf import VERSION

setup(name='fsurf',
      version=VERSION,
      description='Command line tool to submit freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      packages=['fsurfer'],
      scripts=['fsurf', 'fsurf-config'],
      data_files=[('/usr/share/fsurfer/scripts', ["bash/autorecon1.sh",
                                                  "bash/autorecon2.sh",
                                                  "bash/autorecon2-whole.sh",
                                                  "bash/autorecon3.sh",
                                                  "bash/autorecon-all.sh"])],
      license='Apache 2.0')
