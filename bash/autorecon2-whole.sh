#!/usr/bin/env bash

module load freesurfer/5.3.0
if [ $? != 0 ];
then
    source /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash
    module load freesurfer/5.3.0
fi
module load xz/5.2.2
date
start=`date +%s`
WD=$PWD
if [ "$OSG_WN_TMP" != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi
cp $1_recon1_output.tar.xz $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvaf $1_recon1_output.tar.xz
rm $1_recon1_output.tar.xz
recon-all                                                               \
        -s $1                                                           \
        -autorecon2                                                     \
        -openmp $2

cd $SUBJECTS_DIR
tar cJf $WD/$1_recon2_output.tar.xz *
cd $WD

