#!/bin/bash

# create fsurfer lib rpms
work_dir=`mktemp -d`
cur_dir=$PWD
echo "Scratch dir: $work_dir"
cp -a * $work_dir
cp -a ../bash $work_dir
echo "Generating package"
cd $work_dir
rm MANIFEST
python setup_fsurfer.py bdist_rpm
cp dist/*.rpm $cur_dir
rm -fr $work_dir

# create fsurfer script rpms
cd $cur_dir
work_dir=`mktemp -d`
echo "Scratch dir: $work_dir"
cp -a * $work_dir
cp -a ../bash $work_dir
echo "Generating package"
cd $work_dir
rm MANIFEST
mv fsurf-osgconnect fsurf
python setup_osgconnect.py bdist_rpm
cp dist/*.rpm $cur_dir
rm -fr $work_dir

# create fsurfer script rpms
cd $cur_dir
work_dir=`mktemp -d`
echo "Scratch dir: $work_dir"
cp -a * $work_dir
cp -a ../bash $work_dir
echo "Generating package"
cd $work_dir
rm MANIFEST
mv fsurf-osgconnect fsurf
python setup_fsurfer_backend.py bdist_rpm
cp dist/*.rpm $cur_dir
rm -fr $work_dir
