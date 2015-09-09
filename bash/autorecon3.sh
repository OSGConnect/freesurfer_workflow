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
cp $1_recon2_*.tar.gz $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvzf $1_recon2_lh_output.tar.gz
tar xvzf $1_recon2_rh_output.tar.gz

recon-all                                                               \
        -s $1                                                           \
        -autorecon3                                                     \
        -openmp $3

tar cvzf $WD/$1_output.tar.gz $SUBJECTS_DIR/*