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
cp $1_recon1_output.tar.xz $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvaf $1_recon1_output.tar.xz
rm $1_recon1_output.tar.xz
recon-all                                                               \
        -s $1                                                           \
        -autorecon2                                                     \
        -openmp $2



tar cJf $WD/$1_recon2_output.tar.xz $SUBJECTS_DIR/*