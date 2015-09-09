#!/usr/bin/env bash
module load freesurfer/5.3.0
date
start=`date +%s`
WD=$PWD
if [ $OSG_WN_TMP != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi
######################################################################## run all steps
recon-all                                                               \
        -all                                                            \
        -s $1                                                           \
        -i $2                                                           \
        -openmp $3


tar cvzf $WD/$1_output.tar.gz $SUBJECTS_DIR/*



