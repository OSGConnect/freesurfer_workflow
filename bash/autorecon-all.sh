#!/usr/bin/env bash
module load freesurfer/5.3.0
######################################################################## To start the job:
########################################################################   sbatch JobFile
######################################################################## NOTE: SLURM directives may be left untouched
########################################################################       when executing locally.
######################################################################## Edit these SLURM directives
## echo "#SBATCH                --job-name=${Q}Daisy_${Sub}${Q}"
#SBATCH         --job-name="Daisy"
# #SBATCH               --output="/home/donkri/Contrib/FreeSpawn/Daisy_${Sub}_${nCore}_All.log"
#SBATCH         -t 03:00:00
######################################################################## Do not edit these SLURM directives
#SBATCH         --partition=shared
#SBATCH         --nodes=1
## echo "#SBATCH                --ntasks-per-node=${nCore}"
#SBATCH         --ntasks-per-node=9
#SBATCH         --export=ALL
#SBATCH         --mem=16G
######################################################################## Do not edit
date
start=`date +%s`
WD=$PWD
if [ $OSG_WN_TMP != "" ];
then

    SUBJECTS_DIR=`mktemp -d --tmpdir=$OSG_WN_TMP`
else
    SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
fi
######################################################################## Edit the subject name/number
######################################################################## NOTE: The "\" at the end of a line is a continuation
########################################################################       character.  It must be the last character on the line.
########################################################################       Lines may be added or deleted which include \ at the end.
######################################################################## 1st stage - serial
recon-all                                                               \
        -all                                                            \
        -s $1                                                           \
        -i $2                                                           \
        -openmp $3                                                      \
                                                                        >& /dev/null

tar cvzf $WD/$1_output.tar.gz $SUBJECTS_DIR/*



