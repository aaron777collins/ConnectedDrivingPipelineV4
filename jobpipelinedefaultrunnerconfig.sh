#!/bin/bash

# Black        0;30     Dark Gray     1;30
# Red          0;31     Light Red     1;31
# Green        0;32     Light Green   1;32
# Brown/Orange 0;33     Yellow        1;33
# Blue         0;34     Light Blue    1;34
# Purple       0;35     Light Purple  1;35
# Cyan         0;36     Light Cyan    1;36
# Light Gray   0;37     White         1;37

RED='\033[0;31m'
NC='\033[0m' # No Color
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'


NO_NEW_LINE="-nnl"

# function to print green
function printColor {
    # format: <text> <color> [no newline? if -nnl then no newline, otherwise if nothing then newline]
    if [ "$#" -eq 3 ]; then
        if [ "$3" == $NO_NEW_LINE ]; then
            echo -ne "$2$1${NC}"
        fi
    else
        echo -e "$2$1${NC}"
    fi
}


if [ "$#" -lt 2 ]; then
    printColor "Illegal number of parameters" $RED
    printColor "Usage: jobpipelinedefaultrunnerconfig.sh <USERNAME> <FILE> <FILE2> ... <FILEN> [OPTIONAL: -d '<DEPENDENCY>']" $YELLOW
    exit 1
fi

USERNAME=$1
# shift to get rid of the username
shift

# Making list of files from param 2 to N (with an optional -d <DEPENDENCY> at the end) to loop through, possibly depending on the dependency specified
# and then chaining dependencies together (previous file depends on the next file)

# getting a list of the files

# files is a list of strings of their names
FILES=()

DEPENDENCY=""


while [ "$#" -gt 0 ]; do
    case "$1" in
        -d)
            DEPENDENCY="$2"
            shift 2  # Remove -d and its argument
            ;;
        *)
            FILES+=("$1")
            shift  # Remove this argument
            ;;
    esac
done

# # loop through each file and add it to the list
# for ((i = 2; i <= $#; i++))
# do
#     # echo "Looping through file $i"
#     # echo "${!i}"
#     if [ "${!i}" == "-d" ]; then
#         # echo "Found -d"
#         # echo "${!i}"
#         # echo "${!i+1}"
#         DEPENDENCY="${!((i+1))}"
#         # echo "Dependency is $DEPENDENCY"
#         break
#     else
#         FILES+=("${!i}")
#     fi
# done

# loop through each file and run the pipeline
# but make sure to put the first dependency as DEPENDENCY (if specified)

# also, make sure to grep the output of this command to search for the new slurm ID
# "Submitted batch job 39734196"
# we are extracting the job ID from this line

# now looping through each file of the FILES list and running the pipeline
# The first dependency will be the DEPENDENCY variable (if specified)
# but then after that, we pull the dependencies from the results of running the pipeline using grep
# and then we chain the dependencies together

printColor "Running pipeline for user " $CYAN $NO_NEW_LINE
printColor "$USERNAME" $YELLOW
# format the files list
# to have newlines between each file
# and then print it out
printColor "with files:" $CYAN
echo -ne "$YELLOW"
printf '\n%s\n' "${FILES[@]}"
echo -ne "$NC"
if [ ! -z "$DEPENDENCY" ]; then
    printColor "with dependency $DEPENDENCY" $GREEN
fi
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
        printColor "Running pipeline for the file '$NC${FILES[$i]}$CYAN' with dependency $newDependency" $CYAN
    else
        printColor "Running pipeline for the file '$NC${FILES[$i]}$CYAN'" $CYAN
    fi
    # Store output from running the pipeline
    OUTPUT=$(/bin/bash runUserPipeline.sh $USERNAME projects/def-arunita/$USERNAME/ConnectedDrivingPipelineV4 ${FILES[$i]} 7 0 0 10 256 $newDependency)

    # echo output from running the pipeline
    echo "$OUTPUT"

    # Parse out the job ID from the output
    # follows the form: "Submitted batch job 39734196"
    # so we want to use regex to extract the job ID

    newDependency=$(echo $OUTPUT | grep -oP '(?<=Submitted batch job )\d+') # this is the new dependency
    printColor "New dependency is $newDependency" $GREEN
    echo ""
done

printColor "Finished running pipeline" $CYAN

# /bin/bash runUserPipeline.sh $USERNAME projects/def-arunita/$USERNAME/ConnectedDrivingPipelineV4 $FILE 7 0 0 10 256 $DEPENDENCY
