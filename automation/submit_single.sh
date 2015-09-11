#!/usr/bin/env bash

# usage submit_single.sh [# of submits] [# of cores] [workflow_dir] [subject_dir]

curr_dir="$PWD"
cd ../python
subject_files=`ls $4/*_defaced.mgz`
subjects=""
for file in subject_files;
do
  subject_name=`basename $file | sed 's/\(.*\)_defaced.mgz/\1/' `
  subjects="$subjects,$subject_name"
done
for i in {1..$1}
do
    ./freesurfer.py --single --Sub $subjects --subject_dir=$4 --nCore $2
    sleep 1
done

for i in single*.xml
do
    pegasus-plan --conf pegasusrc    \
                 --sites condorpool  \
                 --dir $3            \
                 --output-site local \
                 --dax $i            \
                 --submit
done
rm single*.xml