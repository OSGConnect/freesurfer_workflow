#!/bin/bash

work_dir=`mktemp -d`
cur_dir=$PWD
echo "Scratch dir: $work_dir"
cp -a * $work_dir
cp -a ../bash $work_dir
"Generating package"
cd $work_dir
mv fsurf-osgconnect fsurf
python setup.py bdist_rpm
cp dist/*.rpm $cur_dir
rm -fr $work_dir
