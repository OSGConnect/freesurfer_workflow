#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - file with subject dir
# $4 - num of cores to use
# $5 - options

module load freesurfer/$1
if [ $? != 0 ];
then
    source /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash
    module load freesurfer/$1
fi
module load xz/5.2.2
date
start=`date +%s`
subject=$2
subject_file=$3
cores=$4
WD=$PWD
if [ "$OSG_WN_TMP" != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi
cp $subject_file $SUBJECTS_DIR
cd $SUBJECTS_DIR
unzip $subject_file
rm $subject_file
shift 4
######################################################################## run all steps
recon-all                                                           \
        $@                                                          \
        -subjid $subject                                            \
        -openmp $cores

cd $SUBJECTS_DIR
cp $subject/scripts/recon-all.log $WD
tar cjf $WD/${subject}_output.tar.bz2 *
cd $WD


