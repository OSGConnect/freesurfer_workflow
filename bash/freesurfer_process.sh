#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - num of cores to use
# $4 - options

module load freesurfer/$1
if [ $? != 0 ];
then
    source /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash
    module load freesurfer/$1
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
cp $2 $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvaf $2
rm $2
######################################################################## run all steps
recon-all                                                           \
        $4                                                          \
        -openmp $3

cd  $SUBJECTS_DIR
tar cJf $WD/$2_output.tar.xz *
cd $WD


