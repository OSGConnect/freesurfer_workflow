#!/usr/bin/env bash
module load freesurfer/5.3.0
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
######################################################################## run all steps
recon-all                                                               \
        -all                                                            \
        -s $1                                                           \
        -i $2                                                           \
        -openmp $3

cd  $SUBJECTS_DIR
tar cJf $WD/$1_output.tar.xz *
cd $WD


