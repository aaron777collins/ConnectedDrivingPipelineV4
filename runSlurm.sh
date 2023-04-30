#!/bin/bash
#SBATCH -J MachineLearningPipelineV4
#SBATCH --account=def-arunita
#SBATCH --cpus-per-task=6
#SBATCH --mem=32G # 512GiB of memery
#SBATCH -t 0-08:00 # Running time of 10 min is 0-00:10
source ENV/bin/activate
python somefile.py
