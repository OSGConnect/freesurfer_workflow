#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - file with subject dir
# $4 - num of cores to use
# $5 - options

command -v module
if [[ $? -ne 0 ]];
then
    source /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash
fi

# load tcsh if not present
command -v tcsh
if [[ $? -ne 0 ]];
then
    module load tcsh/6.20.00
fi

module load freesurfer/$1
module load xz/5.2.2
date
start=`date +%s`
subject=$2
subject_file=$3
cores=$4
WD=$PWD
if [ -d "$OSG_WN_TMP" ];
then
    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    # OSG_WN_TMP doesn't exist or isn't defined
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi
cp $subject_file $SUBJECTS_DIR
cd $SUBJECTS_DIR
unzip $subject_file
rm $subject_file
shift 4
exitcode=0
######################################################################## run all steps
recon-all                                                           \
        $@                                                          \
        -subjid $subject                                            \
        -openmp $cores
if [ $? -ne 0 ];
then
  exitcode=1
fi
cd $SUBJECTS_DIR
cp $subject/scripts/recon-all.log $WD
tar cjf $WD/${subject}_output.tar.bz2 *
cd $WD
exit $exitcode

