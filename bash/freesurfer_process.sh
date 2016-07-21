#!/usr/bin/env bash
module load freesurfer/$4
if [ $? != 0 ];
then
    source /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash
    module load freesurfer/$4
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
cp $1 $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvaf $1
rm $1
######################################################################## run all steps
recon-all                                                           \
        $2                                                          \
        -openmp $3

cd  $SUBJECTS_DIR
tar cJf $WD/$1_output.tar.xz *
cd $WD


