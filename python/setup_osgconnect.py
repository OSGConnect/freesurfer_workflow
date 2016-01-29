#!/usr/bin/env python

# Copyright 2015 University of Chicago
# Available under Apache 2.0 License
# setup for fsurf on OSG Connect login
from distutils.core import setup
import subprocess

# workaround missing subprocess.check_output
if "check_output" not in dir(subprocess):  # duck punch it in!
    def check_output(*popenargs, **kwargs):
        """
        Run command with arguments and return its output as a byte string.

        Backported from Python 2.7 as it's implemented as pure python
        on stdlib.

        """
        process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            error = subprocess.CalledProcessError(retcode, cmd)
            error.output = output
            raise error
        return output

    subprocess.check_output = check_output

version_string = subprocess.check_output(['./fsurf', '--version'], stderr=subprocess.STDOUT)
version_string = version_string[6:-1]
setup(name='fsurf',
      version=version_string,
      description='Command line tool to submit freesurfer workflows',
      author='Suchandra Thapa',
      author_email='sthapa@ci.uchicago.edu',
      url='https://github.com/OSGConnect/freesurfer_workflow',
      scripts=['fsurf', 'fsurf-config', 'email_fsurf_notification.py'],
      license='Apache 2.0')


def check_output(*popenargs, **kwargs):
    """
    Run command with arguments and return its output as a byte string.

    Backported from Python 2.7 as it's implemented as pure python
    on stdlib.

    """
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output
