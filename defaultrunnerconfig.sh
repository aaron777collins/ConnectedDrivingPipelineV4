#!/bin/bash


if [ "$#" -lt 2 ]; then
    echo "Illegal number of parameters"
    echo "Usage: defaultrunnerconfig.sh <FILE> <USERNAME> [OPTIONAL: DEPENDENCY]"
    exit 1
fi

FILE=$1
USERNAME=$2

# if dependency is not provided, then set it to an empty string
if [ "$#" -eq 3 ]; then
    DEPENDENCY="$3"
else
    DEPENDENCY=""
fi

/bin/bash runUserPipeline.sh $USERNAME projects/def-arunita/$USERNAME/ConnectedDrivingPipelineV4 $FILE 7 0 0 10 256 $DEPENDENCY
