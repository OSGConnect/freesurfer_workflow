#!/usr/bin/env bash
# arguments
# $1 - Freesurfer version
# $2 - subject name
# $3 - hemisphere to analyze
# $4 - num of cores to use
# $5 - FreeSurfer options

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
freesurfer_args=$5
WD=$PWD
if [ -d "$OSG_WN_TMP" ];
then
    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    # OSG_WN_TMP doesn't exist or isn't defined
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi

cp $2_recon1_output.tar.xz $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvaf $2_recon1_output.tar.xz
rm $2_recon1_output.tar.xz
exitcode=0
if [[ $version == "5.1.0" ]];
then
    recon-all                                                               \
            -s $2                                                           \
            -autorecon2-perhemi                                             \
            -hemi $3                                                        \
            $freesurfer_args
else
    recon-all                                                               \
            -s $2                                                           \
            -autorecon2-perhemi                                             \
            -hemi $3                                                        \
            -openmp $4                                                      \
            $freesurfer_args
fi

if [ $? -ne 0 ];
then
  exitcode=1
fi
cd $SUBJECTS_DIR
mv $2/scripts/recon-all.log $2/scripts/recon-all-step2-$3.log
tar cJf $WD/$2_recon2_$3_output.tar.xz *
cd $WD
exit $exitcode