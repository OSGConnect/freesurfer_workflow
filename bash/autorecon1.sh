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
WD=$PWD
if [ "$OSG_WN_TMP" != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi

IFS=',' read -r -a input_files <<< "$4"
$input_args = ""
for index in "${!input_files[@]}";
do
    input_args="$input_args -i ${input_files[index]}"
done
######################################################################## 1st stage - serial
recon-all                                                               \
        -s $2                                                           \
        $input_args                                                     \
        -autorecon1                                                     \
        -openmp $3

######################################################################## 2nd stage - serial
SW="autorecon2-volonly"
recon-all                                                               \
        -s $1                                                           \
        -autorecon2-volonly                                             \
        -openmp $3

cd ${SUBJECTS_DIR}
mv $1/scripts/recon-all.log $1/scripts/recon-all-step1.log
tar cJf ${WD}/$1_recon1_output.tar.xz *
cd ${WD}

