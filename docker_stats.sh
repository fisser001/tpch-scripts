#!/bin/bash

echo $(date '+%d/%m/%Y %H:%M:%S.%3N') -a >> impala_denormal_query_sf1_stats.csv
docker stats -a >> impala_denormal_query_sf1_stats.csv