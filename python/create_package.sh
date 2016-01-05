#!/bin/bash

work_dir=`mktemp -d`
cur_dir=$PWD
cp -a * $work_dir
cp -a ../bash $work_dir
cd $work_dir
mv fsurf-osgconnect fsurf
python setup.py bdist_rpm
rm -fr $work_dir
