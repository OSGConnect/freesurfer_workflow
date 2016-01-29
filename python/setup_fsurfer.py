#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurfer-libs
from distutils.core import setup

import fsurfer


setup(name='fsurfer-libs',
      version=fsurfer.__version__,
      description='Python module to help create freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      packages=['fsurfer'],
      data_files=[('/usr/share/fsurfer/scripts', ["bash/autorecon1.sh",
                                                  "bash/autorecon2.sh",
                                                  "bash/autorecon2-whole.sh",
                                                  "bash/autorecon3.sh",
                                                  "bash/autorecon-all.sh"])],
      license='Apache 2.0')
