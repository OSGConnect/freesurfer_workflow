#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License

import os
from distutils.core import setup


SCRIPT_PATH = os.path.abspath('../bash')
setup(name='fsurf',
      version='0.2',
      description='Command line tool to submit freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      packages=['fsurfer'],
      scripts=['fsurf-osgconnect', 'fsurf-config'],
      data_files=[('/usr/share/fsurfer/scripts', [os.path.join(SCRIPT_PATH,
                                                               'autorecon1.sh'),
                                                  os.path.join(SCRIPT_PATH,
                                                               'autorecon2.sh'),
                                                  os.path.join(SCRIPT_PATH,
                                                               'autorecon2-whole.sh'),
                                                  os.path.join(SCRIPT_PATH,
                                                               'autorecon3.sh'),
                                                  os.path.join(SCRIPT_PATH,
                                                               'autorecon-all.sh')])],
      license='Apache 2.0')
