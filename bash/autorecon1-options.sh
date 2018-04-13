#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - num of cores to use
# $4 - args to freesurfer
# $5 - input file (zip file with subject dir)

command -v module
if [[ $? -ne 0 ]];
then
    source /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash
fi
version=$1
module load freesurfer/$version
module load xz/5.2.2
date
start=`date +%s`
subject=$2
cores=$3
freesurfer_args=$4
subject_file=$5
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

exitcode=0
############################################################ 1st stage - serial
if [[ $version == "5.1.0" ]];
then
    recon-all                                                               \
            -s $subject                                                     \
            $input_args                                                     \
            -autorecon1                                                     \
            $freesurfer_args
else
    recon-all                                                               \
            -s $subject                                                     \
            $input_args                                                     \
            -autorecon1                                                     \
            -openmp $cores                                                  \
            $$freesurfer_args
fi

if [ $? -ne 0 ];
then
  exit 1
fi
############################################################ 2nd stage - serial
if [[ $version == "5.1.0" ]];
then
    recon-all                                                               \
            -s $subject                                                     \
            -autorecon2-volonly
            $freesurfer_args
else
    recon-all                                                               \
            -s $subject                                                     \
            -autorecon2-volonly                                             \
            -openmp $cores
            $freesurfer_args
fi

if [ $? -ne 0 ];
then
  exitcode=1
fi

cd ${SUBJECTS_DIR}
mv $subject/scripts/recon-all.log $subject/scripts/recon-all-step1.log
tar cJf ${WD}/${subject}_recon1_output.tar.xz *
cd ${WD}

exit $exitcode