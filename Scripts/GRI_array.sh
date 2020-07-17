#!/bin/bash
#SBATCH --job-name=GRI_Array
#SBATCH --time=15:00:00         # walltime
#SBATCH --output=Out.%J.%a      # Name of the output log file
#SBATCH --error=Err.%J.%a       # Name of the error log file
#SBATCH --nodes=1               # Number of nodes
#SBATCH --ntasks-per-node=1     # Tasks per node
#SBATCH --exclusive             # Don't share compute node with anyone
#SBATCH --mail-type=END,FAIL 
#SBATCH --mail-user=j.j.nieves@soton.ac.uk

cd ${SLURM_SUBMIT_DIR}


TMPDIR=/tmp

TMP=$TMPDIR
TEMP=$TMPDIR

# Retrieve our argument row (accounting for the header row by adding 1)
ARG_ID = $SLURM_ARRAY_TASK_ID+1

export TMPDIR TMP TEMP

ulimit -s unlimited


PRJPATH=/mainfs/scratch/jjn1n15/GRI/





# Declare our input argument file. Each row is a job where the arguments are as follows.
# YEAR - year of interest to process
# CORES - number of cores to use in the job
# THRESHOLD - the population count threshold that was used to generate the input rasters
# MAKESUM - Create a sum of 0-1 normalized layers? (Logical, TRUE or FALSE)
INFILE=inGRI.csv
# Declare that our divider is a :, that we want to only read the given task, 
# and to retrieve the variables:
awk FS=':' 'NR==$ARG_ID' YEAR="$0" CORES="$1" THRESHOLD="$2" MAKESUM="$3" $INFILE


Rscript --no-restore --no-save --vanilla --slave giniCalc_HPC.R $YEAR $CORES $THRESHOLD $MAKESUM
			
	