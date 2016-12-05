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
WD=$PWD
if [ "$OSG_WN_TMP" != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi

shift 4
input_args=""
while (( "$#" ));
do
    input_args="$input_args -i $1"
    shift
done
exitcode=0
######################################################################## run all steps
recon-all                                                               \
        -all                                                            \
        -s $2                                                           \
        $input_args                                                     \
        -openmp $3
if [ $? -ne 0 ];
then
  exitcode=1
fi
cd  $SUBJECTS_DIR
cp $2/scripts/recon-all.log $WD
tar cjf $WD/$1_output.tar.bz2 *
cd $WD

exit $exitcode
