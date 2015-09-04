#!/usr/bin/env bash
nCore=9
while (( "$#" ));
do
    case $1 in
    --Sub)
        Sub=$2
        shift 2
        ;;
    --nCore)
        nCore=$2
        shift 2
        ;;
    --tokenID)
        tokenID=$2
        shift 2
        ;;
    --hemi)
        hemi=$2
        shift 2
        ;;
    --debug)
        debug=1
        shift
        ;;
    --log)
        log=$2
        shift
        ;;
    *)
        echo "Unrecognized argument: $1"
        exit 1
    esac
done

if [ -z "$nCore" -o -z "$Sub" ];
then
    echo "Sub and nCore must be set."
    echo "e.g. squeue DaisyChain_N_All.sh --Sub 052 --nCore 9"
    exit 1
fi


if [ -n "$debug" ];
then
    echo "Sub: $Sub"
    echo "nCore: $nCore"
    echo "SkipRecon: $SkipRecon"
    echo "tokenID: $tokenID"
    echo "hemi: $hemi"
fi