set hive.execution.engine=mr;
set hive.exec.mode.local.auto;
set mapred.job.tracker;
SET mapreduce.framework.name=yarn;
set mapreduce.tasktracker.http.address=0.0.0.0:50060;

-- Normalized HIVE / PRESTO / DRILL
CREATE TABLE c_customer_tmp (c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string, c_acctbal double, c_mktsegment string, c_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE l_lineitem_tmp (l_orderkey string, l_partkey string, l_suppkey string, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, l_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE n_nation_tmp (n_nationkey string, n_name string, n_regionkey string,n_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE r_region_tmp (r_regionkey string, r_name string, r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE o_orders_tmp (o_orderkey string, o_custkey string, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int ,o_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE p_part_tmp (p_partkey string, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE s_supplier_tmp (s_suppkey string, s_name string, s_address string, s_nationkey string, s_phone string, s_acctbal double, s_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE ps_partsupp_tmp (ps_partkey string, ps_suppkey string, ps_availqty int, ps_supplycost double, ps_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/customer' OVERWRITE INTO TABLE c_customer_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/lineitems' OVERWRITE INTO TABLE l_lineitem_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/nation' OVERWRITE INTO TABLE n_nation_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/region' OVERWRITE INTO TABLE r_region_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/order' OVERWRITE INTO TABLE o_orders_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/part' OVERWRITE INTO TABLE p_part_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/supplier' OVERWRITE INTO TABLE s_supplier_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/partsupp' OVERWRITE INTO TABLE ps_partsupp_tmp;

--Normalized Impala
CREATE TABLE c_customer_tmp (c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string, c_acctbal double, c_mktsegment string, c_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE l_lineitem_tmp (l_orderkey string, l_partkey string, l_suppkey string, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string, l_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE n_nation_tmp (n_nationkey string, n_name string, n_regionkey string,n_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE r_region_tmp (r_regionkey string, r_name string, r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE o_orders_tmp (o_orderkey string, o_custkey string, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int ,o_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE p_part_tmp (p_partkey string, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE s_supplier_tmp (s_suppkey string, s_name string, s_address string, s_nationkey string, s_phone string, s_acctbal double, s_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE ps_partsupp_tmp (ps_partkey string, ps_suppkey string, ps_availqty int, ps_supplycost double, ps_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;


LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/customer' OVERWRITE INTO TABLE c_customer_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/lineitems' OVERWRITE INTO TABLE l_lineitem_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/nation' OVERWRITE INTO TABLE n_nation_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/region' OVERWRITE INTO TABLE r_region_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/order' OVERWRITE INTO TABLE o_orders_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/part' OVERWRITE INTO TABLE p_part_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/supplier' OVERWRITE INTO TABLE s_supplier_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/normal/partsupp' OVERWRITE INTO TABLE ps_partsupp_tmp;

--ORC
Create Table c_customer stored as orc as select * from c_customer_tmp;
Create Table l_lineitem stored as orc as select * from l_lineitem_tmp;
Create Table n_nation stored as orc as select * from n_nation_tmp;
Create Table r_region stored as orc as select * from r_region_tmp;
Create Table o_orders stored as orc as select * from o_orders_tmp;
Create Table p_part stored as orc as select * from p_part_tmp;
Create Table s_supplier stored as orc as select * from s_supplier_tmp;
Create Table ps_partsupp stored as orc as select * from ps_partsupp_tmp;

--PARQUET
Create Table c_customer stored as parquet as select * from c_customer_tmp;
Create Table l_lineitem stored as parquet as select * from l_lineitem_tmp;
Create Table n_nation stored as parquet as select * from n_nation_tmp;
Create Table r_region stored as parquet as select * from r_region_tmp;
Create Table o_orders stored as parquet as select * from o_orders_tmp;
Create Table p_part stored as parquet as select * from p_part_tmp;
Create Table s_supplier stored as parquet as select * from s_supplier_tmp;
Create Table ps_partsupp stored as parquet as select * from ps_partsupp_tmp;


drop table c_customer_tmp;
drop table l_lineitem_tmp;
drop table n_nation_tmp;
drop table r_region_tmp;
drop table o_orders_tmp;
drop table p_part_tmp;
drop table s_supplier_tmp;
drop table ps_partsupp_tmp;

drop table c_customer;
drop table l_lineitem;
drop table n_nation;
drop table r_region;
drop table o_orders;
drop table p_part;
drop table s_supplier;
drop table ps_partsupp;

show tables;

--Denormalized
CREATE TABLE denormalized_tmp (c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string, c_acctbal double, c_mktsegment string, c_comment string, n_nationkey string, n_name string, n_regionkey string, n_comment string, r_regionkey string, r_name string, r_comment string, o_orderkey string, o_custkey string, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string, l_orderkey string, l_partkey string, l_suppkey string, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, l_comment string, s_suppkey string, s_name string, s_address string, s_nationkey string, s_phone string, s_acctbal double, s_comment string, n2_nationkey string, n2_name string, n2_regionkey string, n2_comment string, r2_regionkey string, r2_name string, r2_comment string, p_partkey string, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string, ps_partkey string, ps_suppkey string, ps_availqty int, ps_supplycost double, ps_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;

--impala
CREATE TABLE denormalized_tmp (c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string, c_acctbal double, c_mktsegment string, c_comment string, n_nationkey string, n_name string, n_regionkey string, n_comment string, r_regionkey string, r_name string, r_comment string, o_orderkey string, o_custkey string, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string, l_orderkey string, l_partkey string, l_suppkey string, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string, l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string, l_comment string, s_suppkey string, s_name string, s_address string, s_nationkey string, s_phone string, s_acctbal double, s_comment string, n2_nationkey string, n2_name string, n2_regionkey string, n2_comment string, r2_regionkey string, r2_name string, r2_comment string, p_partkey string, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string, ps_partkey string, ps_suppkey string, ps_availqty int, ps_supplycost double, ps_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/data/sf1/denorm/' OVERWRITE INTO TABLE denormalized_tmp;

--ORC
Create Table denormalized stored as orc as select * from denormalized_tmp;

--PARQUET
Create Table denormalized stored as parquet as select * from denormalized_tmp;

drop table denormalized_tmp;

select * from denormalized limit 5;


-- star
CREATE TABLE c_customer_tmp_star (c_custkey string , c_name string , c_address string , c_nationkey string , c_phone string , c_acctbal double , c_mktsegment string , c_comment string , n_nationkey string , n_name string , n_regionkey string , n_comment string , r_regionkey string , r_name string , r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE lo_lineitem_orders_tmp_star (o_orderkey string , o_custkey string , o_orderstatus string , o_totalprice double , o_orderdate date , o_orderpriority string , o_clerk string , o_shippriority int , o_comment string , l_orderkey string , l_partkey string , l_suppkey string , l_linenumber int , l_quantity double , l_extendedprice double , l_discount double , l_tax double , l_returnflag string , l_linestatus string , l_shipdate date , l_commitdate date , l_receiptdate date , l_shipinstruct string , l_shipmode string , l_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE s_supplier_tmp_star (s_suppkey string , s_name string , s_address string , s_nationkey string , s_phone string , s_acctbal double , s_comment string , n_nationkey string , n_name string , n_regionkey string , n_comment string , r_regionkey string , r_name string , r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE p_part_star_tmp (p_partkey string , p_name string , p_mfgr string , p_brand string , p_type string , p_size int , p_container string , p_retailprice double , p_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE ps_partsupp_star_tmp (ps_partkey string , ps_suppkey string , ps_availqty int , ps_supplycost double , ps_comment string , ps_timestamp timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/data/sf1/star/customer' OVERWRITE INTO TABLE c_customer_tmp_star;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/lineitemorders' OVERWRITE INTO TABLE lo_lineitem_orders_tmp_star;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/supplier' OVERWRITE INTO TABLE s_supplier_tmp_star;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/part' OVERWRITE INTO TABLE p_part_star_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/partsupp' OVERWRITE INTO TABLE ps_partsupp_star_tmp;

Create Table c_customer_star stored as orc as select * from c_customer_tmp_star;
Create Table lo_lineitem_orders_star stored as orc as select * from lo_lineitem_orders_tmp_star;
Create Table s_supplier_star stored as orc as select * from s_supplier_tmp_star;
Create Table p_part_star stored as orc as select * from p_part_star_tmp;
Create Table ps_partsupp_star stored as orc as select * from ps_partsupp_star_tmp;

-- star impala
CREATE TABLE c_customer_tmp_star (c_custkey string , c_name string , c_address string , c_nationkey string , c_phone string , c_acctbal double , c_mktsegment string , c_comment string , n_nationkey string , n_name string , n_regionkey string , n_comment string , r_regionkey string , r_name string , r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE lo_lineitem_orders_tmp_star (o_orderkey string , o_custkey string , o_orderstatus string , o_totalprice double , o_orderdate string , o_orderpriority string , o_clerk string , o_shippriority int , o_comment string , l_orderkey string , l_partkey string , l_suppkey string , l_linenumber int , l_quantity double , l_extendedprice double , l_discount double , l_tax double , l_returnflag string , l_linestatus string , l_shipdate string , l_commitdate string , l_receiptdate string , l_shipinstruct string , l_shipmode string , l_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE s_supplier_tmp_star (s_suppkey string , s_name string , s_address string , s_nationkey string , s_phone string , s_acctbal double , s_comment string , n_nationkey string , n_name string , n_regionkey string , n_comment string , r_regionkey string , r_name string , r_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE p_part_star_tmp (p_partkey string , p_name string , p_mfgr string , p_brand string , p_type string , p_size int , p_container string , p_retailprice double , p_comment string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;
CREATE TABLE ps_partsupp_star_tmp (ps_partkey string , ps_suppkey string , ps_availqty int , ps_supplycost double , ps_comment string , ps_timestamp timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|" STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/data/sf1/star/customer' OVERWRITE INTO TABLE c_customer_tmp_star;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/lineitemorders' OVERWRITE INTO TABLE lo_lineitem_orders_tmp_star;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/supplier' OVERWRITE INTO TABLE s_supplier_tmp_star;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/part' OVERWRITE INTO TABLE p_part_star_tmp;
LOAD DATA LOCAL INPATH '/opt/data/sf1/star/partsupp' OVERWRITE INTO TABLE ps_partsupp_star_tmp;

Create Table c_customer_star stored as parquet as select * from c_customer_tmp_star;
Create Table lo_lineitem_orders_star stored as parquet as select * from lo_lineitem_orders_tmp_star;
Create Table s_supplier_star stored as parquet as select * from s_supplier_tmp_star;
Create Table p_part_star stored as parquet as select * from p_part_star_tmp;
Create Table ps_partsupp_star stored as parquet as select * from ps_partsupp_star_tmp;

drop table c_customer_tmp_star;
drop table lo_lineitem_orders_tmp_star;
drop table s_supplier_tmp_star;
drop table p_part_star_tmp;
drop table ps_partsupp_star_tmp;

drop table c_customer_star;
drop table lo_lineitem_orders_star;
drop table s_supplier_star;
drop table p_part_star;
drop table ps_partsupp_star;

select * from c_customer_tmp limit 2;
select * from l_lineitem_tmp limit 2;
select * from n_nation_tmp limit 2;
select * from r_region_tmp limit 2;
select * from o_orders_tmp limit 2;
select * from p_part_tmp limit 2;
select * from s_supplier_tmp limit 2;
select * from ps_partsupp_tmp limit 2;

select * from c_customer limit 2;
select * from l_lineitem limit 2;
select * from n_nation limit 2;
select * from r_region limit 2;
select * from o_orders limit 2;
select * from p_part limit 2;
select * from s_supplier limit 2;
select * from ps_partsupp limit 2;