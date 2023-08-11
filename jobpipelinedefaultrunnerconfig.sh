#!/bin/bash


if [ "$#" -lt 2 ]; then
    echo "Illegal number of parameters"
    echo "Usage: jobpipelinedefaultrunnerconfig.sh <USERNAME> <FILE> <FILE2> ... <FILEN> [OPTIONAL: -d '<DEPENDENCY>']"
    exit 1
fi

USERNAME=$1

# Making list of files from param 2 to N (with an optional -d <DEPENDENCY> at the end) to loop through, possibly depending on the dependency specified
# and then chaining dependencies together (previous file depends on the next file)

# getting a list of the files

# files is a list of strings of their names
FILES=()

DEPENDENCY=""

# loop through each file and add it to the list
for ((i = 2; i <= $#; i++))
do
    # echo "Looping through file $i"
    # echo "${!i}"
    if [ "${!i}" == "-d" ]; then
        # echo "Found -d"
        # echo "${!i}"
        # echo "${!i+1}"
        DEPENDENCY="${!i+1}"
        # echo "Dependency is $DEPENDENCY"
        break
    else
        FILES+=("${!i}")
    fi
done

# loop through each file and run the pipeline
# but make sure to put the first dependency as DEPENDENCY (if specified)

# also, make sure to grep the output of this command to search for the new slurm ID
# "Submitted batch job 39734196"
# we are extracting the job ID from this line

# now looping through each file of the FILES list and running the pipeline
# The first dependency will be the DEPENDENCY variable (if specified)
# but then after that, we pull the dependencies from the results of running the pipeline using grep
# and then we chain the dependencies together

echo "Running pipeline for user $USERNAME"
echo "with files: ${FILES[@]}"
echo "with dependency: $DEPENDENCY"
echo ""

usedDependencies=()

# set dependency to the first dependency to start
newDependency=""
# add the first dependency to the list of used dependencies
if [ ! -z "$DEPENDENCY" ]; then
    usedDependencies+=($DEPENDENCY)
    newDependency=$DEPENDENCY
fi


for ((i = 0; i < ${#FILES[@]}; i++))
do
    if [ ! -z "$DEPENDENCY" ]; then
        echo "Running pipeline for file ${FILES[$i]} with dependency $newDependency"
    else
        echo "Running pipeline for file ${FILES[$i]}"
    fi
    # Store output from running the pipeline
    OUTPUT=$(/bin/bash runUserPipeline.sh $USERNAME projects/def-arunita/$USERNAME/ConnectedDrivingPipelineV4 ${FILES[$i]} 7 0 0 10 256 $newDependency)

    # Parse out the job ID from the output
    # follows the form: "Submitted batch job 39734196"
    # so we want to use regex to extract the job ID

    newDependency=$(echo $OUTPUT | grep -oP '(?<=Submitted batch job )\d+') # this is the new dependency
    echo "New dependency is $newDependency"
    echo ""
done

echo "Finished running pipeline

# /bin/bash runUserPipeline.sh $USERNAME projects/def-arunita/$USERNAME/ConnectedDrivingPipelineV4 $FILE 7 0 0 10 256 $DEPENDENCY
