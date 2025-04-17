#!/usr/bin/bash
#SBATCH --job-name=test_job
#SBATCH --partition=queue
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=20
#SBATCH --time=13:00:00
#SBATCH --output=qlog_test_job
#SBATCH --error=err_test_job
#SBATCH --no-requeue


cd $SLURM_SUBMIT_DIR
source_line

command1
command2
