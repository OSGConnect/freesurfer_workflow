#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurfer-libs
from distutils.core import setup

setup(name='fsurfer-libs',
      version='PKG_VERSION',
      description='Python module to help create freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      packages=['fsurfer'],
      data_files=[('/usr/share/fsurfer/scripts', ["bash/autorecon1.sh",
                                                  "bash/autorecon2.sh",
                                                  "bash/autorecon2-whole.sh",
                                                  "bash/autorecon3.sh",
                                                  "bash/autorecon1-options.sh",
                                                  "bash/autorecon2-options.sh",
                                                  "bash/autorecon3-options.sh",
                                                  "bash/autorecon-all.sh",
                                                  "bash/freesurfer-process.sh"])],
      license='Apache 2.0')
