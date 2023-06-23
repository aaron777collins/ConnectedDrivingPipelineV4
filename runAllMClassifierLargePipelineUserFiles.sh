#!/bin/bash
# gathering the file names of all files starting with MClassifierLargePipelineUser
# and ending with .py
files=$(ls MClassifierLargePipelineUser*.py)

# getting the username of the user by asking
echo "Please enter your username:"
read username

# read password to feed into any inputs asked by ssh
echo "Please enter your password:"
read -s password

# Usage: defaultrunnerconfig.sh <FILE> <USERNAME> [OPTIONAL: DEPENDENCY]
# running the defaultrunnerconfig.sh script for each file with the previous file as a dependency
# we need to keep track of the output of defaultrunnerconfig.sh and parse it to get the dependency
# from sbatch
for file in $files
do
    # getting the dependency from the previous run
    dependency=$(echo $output | grep -oP '(?<=Submitted batch job )[0-9]*')
    # printing the dependency
    echo "Dependency: $dependency"
    # running the defaultrunnerconfig.sh script but with the previous file as a dependency
    # also passing in the username and the dependency
    # and feeding in the password to any ssh commands
    output=$(echo $password | /bin/bash defaultrunnerconfig.sh $file $username $dependency)
    # printing the output of the defaultrunnerconfig.sh script
    echo $output
    # sleeping for 1 second to avoid overloading the scheduler
    sleep 1
done
