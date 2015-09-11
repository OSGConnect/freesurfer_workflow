#!/usr/bin/env bash

# usage submit_single.sh [# of submits] [# of cores] [workflow_dir] [subject_dir]

curr_dir="$PWD"
cd ../python
subjects=""
for i in `ls $4/*_defaced.mgz`;
do
  subject_name=`basename $i | sed 's/\(.*\)_defaced.mgz/\1/'`
  subjects="$subjects$subject_name,"
done
subjects=`echo $subjects | sed 's/.$//'`
for i in `seq $1`
do
    ./freesurfer.py --single --Sub $subjects --subject_dir=$4 --nCore $2
    sleep 1
done

for i in `ls single*.xml`;
do
    pegasus-plan --conf pegasusrc    \
                 --sites condorpool  \
                 --dir $3            \
                 --output-site local \
                 --dax $i            \
                 --submit
done
rm single*.xml
