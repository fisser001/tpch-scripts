#!/bin/bash

if [ ! -d $1 ]; then

fileName=$1
mkdir -p ./results/$fileName
while true
do
echo "---------------------------------------------------"
resultPath=results/$fileName/$fileName"_$(date '+%d-%m-%Y_%H:%M:%S:%3N')".csv
docker stats --no-stream|& tee -a $resultPath
echo "---------------------------------------------------"
echo ""
done
else
echo "Parameters have to be defined for this script. Paramater 1 for filename."
echo "Example: ./docker_stats.sh impala_normal_sf1_stats"
exit 2
fi

