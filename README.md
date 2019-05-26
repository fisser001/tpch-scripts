# tpch-scripts
This repository contains all scripts to perform the benchmark described in this master thesis.

All script contain information which describe how the script can be used. Just try to execute the script and the relevant information are shown.

Before using the script make sure that the execution rights are set. 

The respective scipts are divided into the following subdirectories:

## data
Contains a script for loading the data from the NAS into the benchmarking environment. 

## docker 
Contains a script to create the docker volumes which have been used by the docker containers within this benchmark.

## helper
Contains several helper scripts which have been used during the implementation of the benchmark. Those script are not relevant for executing the benchmark. 

## performance
Contains a script for recording the utilization of the docker containers during the execution of the benchmark. The utilization has been recorded via docker stats.

## queries
Those scripts contain the benchmark queries for each tool and schema in order to automate the execution of the queries. In addition, it contains the script for recoring the execution plans for each tool and schema.

## schema
This directory contains scripts for automatic creation and deletion of the different benchmarking schema. The creation schema differ by the used file format. The creation and deletion of the schema was implemented with the help of Hive on the Hadoop cluster.






