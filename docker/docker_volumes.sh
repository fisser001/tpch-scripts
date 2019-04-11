#!/bin/bash

if [ ! -d $1 ]; then
if [ "$1" = "create" ]; then
docker volume create mydata -d local-persist -o mountpoint=/data/mydata --name mydata
docker volume create hadoop_datanode1 -d local-persist -o mountpoint=/data/hadoop_datanode1
docker volume create hadoop_datanode2 -d local-persist -o mountpoint=/data/hadoop_datanode2
docker volume create hadoop_datanode3 -d local-persist -o mountpoint=/data/hadoop_datanode3
docker volume create hadoop_namenode -d local-persist -o mountpoint=/data/hadoop_namenode
docker volume create hadoop_historyserver -d local-persist -o mountpoint=/data/hadoop_historyserver
docker volume create pgdata -d local-persist -o mountpoint=/data/pgdata
docker volume create hive_metastore -d local-persist -o mountpoint=/data/hive_metastore
docker volume create hadoop_resourcemanager -d local-persist -o mountpoint=/data/rhadoop_resourcemanager
docker volume create hadoop_nodemanager1 -d local-persist -o mountpoint=/data/hadoop_nodemanager1
docker volume create hadoop_nodemanager2 -d local-persist -o mountpoint=/data/hadoop_nodemanager2
fi

if [ "$1" = "delete" ]; then
docker volume rm hadoop_datanode1
docker volume rm hadoop_datanode2
docker volume rm hadoop_datanode3
docker volume rm hadoop_namenode
docker volume rm hadoop_historyserver
docker volume rm pgdata
docker volume rm hive_metastore
docker volume rm hadoop_resourcemanager
docker volume rm hadoop_nodemanager1
docker volume rm hadoop_nodemanager2
fi
else
echo "Please specify a parameters. One of the following parameters can be specified: "
echo "create or delete"
echo "Example: ./docker_volumes.sh create"
fi