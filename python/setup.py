#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License

import os
from distutils.core import setup


setup(name='fsurf',
      version='0.1',
      description='Command line tool to submit freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      packages=['fsurfer'],
      scripts=['fsurf-osgconnect'],
      data_files=[('/usr/share/fsurfer/scripts', ["./bash/autorecon1.sh",
                                                  "./bash/autorecon2.sh",
                                                  "./bash/autorecon2-whole.sh",
                                                  "bash/autorecon3.sh",
                                                  "bash/autorecon-all.sh"])],
      license='Apache 2.0')
