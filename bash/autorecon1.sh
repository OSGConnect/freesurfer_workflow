#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - num of cores to use
# $4 - input files

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
WD=$PWD
if [ -d "$OSG_WN_TMP" ];
then
    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    # OSG_WN_TMP doesn't exist or isn't defined
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi

shift 3
input_args=""
while (( "$#" ));
do
    input_args="$input_args -i $1"
    shift
done
exitcode=0
############################################################ 1st stage - serial
if [[ $version == "5.3.0" ]];
then
    recon-all                                                               \
            -s $subject                                                     \
            $input_args                                                     \
            -autorecon1
else
    recon-all                                                               \
            -s $subject                                                     \
            $input_args                                                     \
            -autorecon1                                                     \
            -openmp $cores
fi

if [ $? -ne 0 ];
then
  exit 1
fi
############################################################ 2nd stage - serial
if [[ $version == "5.3.0" ]];
then
    recon-all                                                               \
            -s $subject                                                     \
            -autorecon2-volonly
else
    recon-all                                                               \
            -s $subject                                                     \
            -autorecon2-volonly                                             \
            -parallel                                                       \
            -openmp $cores
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