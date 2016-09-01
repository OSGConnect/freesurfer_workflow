#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - num of cores to use

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
cp $2_recon2_*.tar.xz $SUBJECTS_DIR
cd $SUBJECTS_DIR
if [ -e "$2_recon2_lh_output.tar.xz" ];
then
    tar xvaf $2_recon2_lh_output.tar.xz
    tar xvaf $2_recon2_rh_output.tar.xz
    rm $2_recon2_lh_output.tar.xz
    rm $2_recon2_rh_output.tar.xz
elif [ -e "$2_recon2_output.tar.xz" ];
then
    tar xvaf $2_recon2_output.tar.xz
    rm $2_recon2_output.tar.xz
fi
recon-all                                                               \
        -s $2                                                           \
        -autorecon3                                                     \
        -openmp $3

cd $SUBJECTS_DIR
mv $2/scripts/recon-all.log $2/scripts/recon-all-step3.log
cat $2/scripts/recon-all-step1.log $2/scripts/recon-all-step2*.log $2/scripts/recon-all-step3.log > $2/scripts/recon-all.log
rm fsaverage lh.EC_average rh.EC_average
tar cjf $WD/$2_output.tar.bz2 *
cp $2/scripts/recon-all.log $WD
cd $WD