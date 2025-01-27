#!/bin/bash
#SBATCH --job-name=GRI_RUS
#SBATCH --time=2:00:00         # walltime
#SBATCH --output=Out.%J         # Name of the output log file
#SBATCH --error=Err.%J          # Name of the error log file
#SBATCH --nodes=1               # Number of nodes
#SBATCH --ntasks-per-node=1     # Tasks per node 
#SBATCH --mem=40G               # Memory requested
#SBATCH --mail-type=END,FAIL 
#SBATCH --mail-user=j.j.nieves@soton.ac.uk

cd ${SLURM_SUBMIT_DIR}


TMPDIR=/tmp

TMP=$TMPDIR
TEMP=$TMPDIR

export TMPDIR TMP TEMP

ulimit -s unlimited


PRJPATH=/mainfs/scratch/jjn1n15/GRI/

YEAR=2015
CORES=1
THRESHOLD=5
MAKESUM=FALSE
ISO_NUM=51

# Declare our input argument file. Each row is a job where the arguments are as follows.
# YEAR - year of interest to process
# CORES - number of cores to use in the job
# THRESHOLD - the population count threshold that was used to generate the input rasters
# MAKESUM - Create a sum of 0-1 normalized layers? (Logical, TRUE or FALSE)
#INFILE=inGRI.csv
# Declare that our divider is a :, that we want to only read the given task, 
# and to retrieve the variables:
#awk FS = ':' 'NR==$ARG_ID' YEAR="$0" CORES="$1" THRESHOLD="$2" MAKESUM="3" $INFILE

# argfile is a text file with space delimited arguments in each row and each row representing a new job. 
# The first argument should be the three letter ISO and the second argument should be the year. Both 
# arguments should be strings as indicated by ''
#INFILE=inGRI.csv
#read YEAR CORES THRESHOLD MAKESUM

Rscript --no-restore --no-save --vanilla --slave gri_GiniCalc_HPC_single_ISO.R $YEAR $CORES $THRESHOLD $MAKESUM $ISO_NUM
			
			

