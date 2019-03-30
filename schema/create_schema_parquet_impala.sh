#!/bin/bash

if [ ! -d $1 ] && [ ! -d $2 ]; then
if [ "$1" = "normal" ] || [ "$1" = "all" ]; then
echo "-------------------------------------- CREATE SCHEMA PARQUET NORMAL ---------------------------------"
echo ""
echo "-------------------------------------- Start creating customer normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE c_customer_tmp (c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string, c_acctbal double, c_mktsegment string, c_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/customer' OVERWRITE INTO TABLE c_customer_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table c_customer stored as parquet as select * from c_customer_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table c_customer_tmp;"
echo "-------------------------------------- Finished creating customer normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating lineitem normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE l_lineitem_tmp (l_orderkey string, l_partkey string, l_suppkey string, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string, l_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/lineitems' OVERWRITE INTO TABLE l_lineitem_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table l_lineitem stored as parquet as select * from l_lineitem_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table l_lineitem_tmp;"
echo "-------------------------------------- Finished creating lineitem normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating nation normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE n_nation_tmp (n_nationkey string, n_name string, n_regionkey string,n_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/nation' OVERWRITE INTO TABLE n_nation_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table n_nation stored as parquet as select * from n_nation_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table n_nation_tmp;"
echo "-------------------------------------- Finished creating nation normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating region normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE r_region_tmp (r_regionkey string, r_name string, r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/region' OVERWRITE INTO TABLE r_region_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table r_region stored as parquet as select * from r_region_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table r_region_tmp;"
echo "-------------------------------------- Finished creating region normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating orders normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE o_orders_tmp (o_orderkey string, o_custkey string, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int ,o_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/order' OVERWRITE INTO TABLE o_orders_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table o_orders stored as parquet as select * from o_orders_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table o_orders_tmp;"
echo "-------------------------------------- Finished creating orders normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating part normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE p_part_tmp (p_partkey string, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/part' OVERWRITE INTO TABLE p_part_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table p_part stored as parquet as select * from p_part_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table p_part_tmp;"
echo "-------------------------------------- Finished creating part normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating supplier normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE s_supplier_tmp (s_suppkey string, s_name string, s_address string, s_nationkey string, s_phone string, s_acctbal double, s_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/supplier' OVERWRITE INTO TABLE s_supplier_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table s_supplier stored as parquet as select * from s_supplier_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table s_supplier_tmp;"
echo "-------------------------------------- Finished creating supplier normal ---------------------------------"
echo ""

echo "-------------------------------------- Start creating partsupp normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE ps_partsupp_tmp (ps_partkey string, ps_suppkey string, ps_availqty int, ps_supplycost double, ps_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/partsupp' OVERWRITE INTO TABLE ps_partsupp_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table ps_partsupp stored as parquet as select * from ps_partsupp_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table ps_partsupp_tmp;"
echo "-------------------------------------- Finished creating partsupp normal ---------------------------------"
echo ""
echo ""
rm -rf /opt/data/sf$2/normal/*
echo ""
fi


if [ "$1" = "star" ] || [ "$1" = "all" ]; then
echo "-------------------------------------- CREATE SCHEMA PARQUET STAR ---------------------------------"
echo ""
echo "-------------------------------------- Start creating customer star ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE c_customer_tmp_star (c_custkey string , c_name string , c_address string , c_nationkey string , c_phone string , c_acctbal double , c_mktsegment string , c_comment string , n_nationkey string , n_name string , n_regionkey string , n_comment string , r_regionkey string , r_name string , r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/star/customer' OVERWRITE INTO TABLE c_customer_tmp_star;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table c_customer_star stored as parquet as select * from c_customer_tmp_star;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table c_customer_tmp_star;"
echo "-------------------------------------- Finished creating customer star ---------------------------------"
echo ""

echo "-------------------------------------- Start creating lineitem star ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE lo_lineitem_orders_tmp_star (o_orderkey string , o_custkey string , o_orderstatus string , o_totalprice double , o_orderdate string , o_orderpriority string , o_clerk string , o_shippriority int , o_comment string , l_orderkey string , l_partkey string , l_suppkey string , l_linenumber int , l_quantity double , l_extendedprice double , l_discount double , l_tax double , l_returnflag string , l_linestatus string , l_shipdate string , l_commitdate string , l_receiptdate string , l_shipinstruct string , l_shipmode string , l_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/star/lineitemorders' OVERWRITE INTO TABLE lo_lineitem_orders_tmp_star;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table lo_lineitem_orders_star stored as parquet as select * from lo_lineitem_orders_tmp_star;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table lo_lineitem_orders_tmp_star;"
echo "-------------------------------------- Finished creating lineitem star ---------------------------------"
echo ""

echo "-------------------------------------- Start creating supplier star ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE s_supplier_tmp_star (s_suppkey string , s_name string , s_address string , s_nationkey string , s_phone string , s_acctbal double , s_comment string , n_nationkey string , n_name string , n_regionkey string , n_comment string , r_regionkey string , r_name string , r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/star/supplier' OVERWRITE INTO TABLE s_supplier_tmp_star;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table s_supplier_star stored as parquet as select * from s_supplier_tmp_star;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table s_supplier_tmp_star;"
echo "-------------------------------------- Finished creating supplier star ---------------------------------"
echo ""

echo "-------------------------------------- Start creating part star ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE p_part_star_tmp (p_partkey string , p_name string , p_mfgr string , p_brand string , p_type string , p_size int , p_container string , p_retailprice double , p_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/star/part' OVERWRITE INTO TABLE p_part_star_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table p_part_star stored as parquet as select * from p_part_star_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table p_part_star_tmp;"
echo "-------------------------------------- Finished creating part star ---------------------------------"
echo ""

echo "-------------------------------------- Start creating partsupp star ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE ps_partsupp_star_tmp (ps_partkey string , ps_suppkey string , ps_availqty int , ps_supplycost double , ps_comment string , ps_timestamp timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/star/partsupp' OVERWRITE INTO TABLE ps_partsupp_star_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table ps_partsupp_star stored as parquet as select * from ps_partsupp_star_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table ps_partsupp_star_tmp;"
echo "-------------------------------------- Finished creating partsupp star ---------------------------------"
echo ""
echo ""
rm -rf /opt/data/sf$2/star/*
echo ""
fi


if [ "$1" = "denormal" ] || [ "$1" = "all" ]; then
echo "-------------------------------------- CREATE SCHEMA PARQUET DENORMALIZED ---------------------------------"
echo ""
echo "-------------------------------------- Start creating denormalized ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; CREATE TABLE denormalized_tmp (c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string, c_acctbal double, c_mktsegment string, c_comment string, n_nationkey string, n_name string, n_regionkey string, n_comment string, r_regionkey string, r_name string, r_comment string, o_orderkey string, o_custkey string, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string, l_orderkey string, l_partkey string, l_suppkey string, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string, l_comment string, s_suppkey string, s_name string, s_address string, s_nationkey string, s_phone string, s_acctbal double, s_comment string, n2_nationkey string, n2_name string, n2_regionkey string, n2_comment string, r2_regionkey string, r2_name string, r2_comment string, p_partkey string, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string, ps_partkey string, ps_suppkey string, ps_availqty int, ps_supplycost double, ps_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; LOAD DATA LOCAL INPATH '/opt/data/sf1/denorm/' OVERWRITE INTO TABLE denormalized_tmp;"
echo ""
rm -rf /opt/data/sf1/denorm/*
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; Create Table denormalized stored as parquet as select * from denormalized_tmp;"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table denormalized_tmp;"
echo "-------------------------------------- Finished creating denormalized ---------------------------------"
echo ""
fi
echo "Existing Tables:"
echo ""
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; show tables;"
echo ""
else
echo "Please specify the parameters. One of the following parameters can be specified for the first parameter. Second paramter defines the scale factor."
echo "all or normal or star or denormal"
echo "Example: ./create_schema_parquet normal 1"
fi










