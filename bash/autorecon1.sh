#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - num of cores to use
# $4 - input files


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
cores=$3
WD=$PWD
if [ "$OSG_WN_TMP" != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi

shift 3
input_args=""
while (( "$#" ));
do
    input_args="$input_args -i $1"
    shift
done
######################################################################## 1st stage - serial
recon-all                                                               \
        -s $subject                                                     \
        $input_args                                                     \
        -autorecon1                                                     \
        -openmp $cores

######################################################################## 2nd stage - serial
SW="autorecon2-volonly"
recon-all                                                               \
        -s $subject                                                     \
        -autorecon2-volonly                                             \
        -openmp $cores

cd ${SUBJECTS_DIR}
mv $subject/scripts/recon-all.log $subject/scripts/recon-all-step1.log
tar cJf ${WD}/${subject}_recon1_output.tar.xz *
cd ${WD}

