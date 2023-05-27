#!/bin/bash




# check for the required arguments to run the pipeline
if [ "$#" -lt 8 ]; then
    echo "Illegal number of parameters"
    echo "Usage: runUserPipeline.sh <USERNAME> <PATH_TO_REPO> <FILE> <DAYS> <HOURS> <MINUTES> <CPUS> <MEM> [OPTIONAL: DEPENDENCY]"
    echo "You entered $# parameters"
    echo "You entered: $@"
    exit 1
fi

USERNAME=$1
PATH_TO_REPO=$2
FILE=$3
DAYS=$4
HOURS=$5
MINUTES=$6
CPUS=$7
MEM=$8

if [ "$#" -eq 9 ]; then
    echo "Dependency provided: $9"
    echo "Setting dependency to --dependency=afterok:$9"
    DEPENDENCY="--dependency=afterok:$9"
else
    DEPENDENCY=""
fi

L1="echo \"Going to folder $PATH_TO_REPO and running git pull\""
L2="cd \"$PATH_TO_REPO\" && git pull"
L3="echo \"Generating the file to run the pipeline\""
L4="python3 /home/$USERNAME/$PATH_TO_REPO/generateSlurm.py \"$FILE\" \"$DAYS\" \"$HOURS\" \"$MINUTES\" \"$CPUS\" \"$MEM\" \"$DEPENDENCY\""
L5="echo \"Generated $FILE.sh\""
L6="echo \"Queueing the pipeline\""
L7="sbatch \"slurmstart-$FILE.sh\""
L8="sq"
L9="echo \"Done\""

# ssh into the user's account
# and generate the file to run the pipeline
# Then queue the pipeline
ssh -T "$USERNAME"@beluga.computecanada.ca << EOL
    $L1 && \
    $L2 && \
    $L3 && \
    $L4 && \
    $L5 && \
    $L6 && \
    $L7 && \
    $L8 && \
    $L9 && \
    echo "Exiting"
EOL


