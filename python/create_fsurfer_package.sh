#!/bin/bash

VERSION="2.0.24"
# create fsurfer lib rpms
work_dir=`mktemp -d`
cur_dir=$PWD
echo "Scratch dir: $work_dir"
cp -a * $work_dir
cp -a ../bash $work_dir
echo "Generating package"
cd $work_dir
rm MANIFEST
sed -i'' "s/PKG_VERSION/$VERSION/" setup_fsurfer.py
sed -i'' "s/PKG_VERSION/$VERSION/" fsurfer/__init__.py
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
sed -i'' "s/PKG_VERSION/$VERSION/" setup_osgconnect.py
sed -i'' "s/PKG_VERSION/$VERSION/" fsurfer/__init__.py
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
sed -i'' "s/PKG_VERSION/$VERSION/" setup_fsurfer_backend.py
sed -i'' "s/PKG_VERSION/$VERSION/" fsurfer/__init__.py
python setup_fsurfer_backend.py bdist_rpm
cp dist/*.rpm $cur_dir
rm -fr $work_dir
