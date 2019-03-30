#!/bin/bash

if [ ! -d $1 ] && [ ! -d $2 ]; then
mkdir -p results
cd results
resultPath=drill_denormal_query_sf"$1"_result.txt
rm -f $resultPath
touch $resultPath
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------"|& tee -a $resultPath|& tee -a $resultPath
echo "-------------------------------------- "$resultPath" ---------------------------------"|& tee -a $resultPath|& tee -a $resultPath

echo "-------------------------------------- DRILL QUERY DENORMAL ---------------------------------"|& tee -a $resultPath|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo ""|& tee -a $resultPath

for i in $(seq 1 $2)
do
echo "--------------------------------------STARTED QUERY 1."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,  sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,  avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc, count(*) as count_order  from hive.denormalized  where l_shipdate <> '""' and l_shipdate <= date '1998-12-01' - interval '90' day  group by l_returnflag, l_linestatus  order by l_returnflag, l_linestatus;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 1."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 2."$i" ---------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 2."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 3."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select o_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority  from hive.denormalized where c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15'  and l_shipdate <> '""' and l_shipdate > date '1995-03-15'  group by o_orderkey, o_orderdate, o_shippriority  order by revenue desc, o_orderdate limit 10;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 3."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 4."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select o_orderpriority, count (distinct o_orderkey) as order_count  from hive.denormalized where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month  and l_commitdate < l_receiptdate  group by o_orderpriority  order by o_orderpriority;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 4."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 5."$i" ---------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 5."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 6."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select sum(l_extendedprice*l_discount) as revenue from hive.denormalized where l_shipdate <> '""' and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 6."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 7."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select  n2_name, n_name,   year(l_shipdate) as l_year, sum(l_extendedprice * (1 - l_discount)) as revenue from hive.denormalized where ((n2_name = 'FRANCE' and n_name = 'GERMANY') or (n2_name = 'GERMANY'  and n_name = 'FRANCE')) and l_shipdate <> '""' and l_shipdate between date '1995-01-01' and date '1996-12-31'  group by n2_name, n_name, year(l_shipdate)  order by n2_name, n_name, l_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 7."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 8."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share  from ( select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume,  n2_name as nation  from hive.denormalized where r_name = 'AMERICA' and o_orderdate between date '1995-01-01'  and date '1996-12-31'  and p_type = 'ECONOMY ANODIZED STEEL') as all_nations  group by o_year  order by  o_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 8."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 9."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select nation, o_year, sum(amount) as sum_profit from ( select n2_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from hive.denormalized where p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 9."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 10."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal,  n_name, c_address, c_phone, c_comment from hive.denormalized  where o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month  and l_returnflag = 'R'  group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment  order by revenue  desc limit 20;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 10."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 11."$i" ---------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 11."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 12."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from hive.denormalized where l_shipdate <> '""' and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 12."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 13."$i" ---------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 13."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 14."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from hive.denormalized where l_partkey = p_partkey and l_shipdate <> '""' and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 14."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 15."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select l_suppkey as supplier_no,sum(l_extendedprice * (1 - l_discount)) as total_revenue, s_name,s_address,s_phone from hive.denormalized  where l_shipdate <> '""' and l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month group by l_suppkey, s_name,s_address,s_phone order by total_revenue desc limit 1;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 15."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 16."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select p_brand, p_type, p_size, count(distinct s_suppkey) as supplier_cnt  from hive.denormalized  where p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%'  and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and s_comment not like '%Customer%Complaints%' group by p_brand, p_type, p_size  order by  supplier_cnt desc,  p_brand,  p_type, p_size;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 16."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 17."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select sum(l_extendedprice) / 7.0 as avg_yearly from hive.denormalized a where p_brand = 'Brand#23' and p_container = 'MED BOX'  and l_quantity < ( select 0.2 * avg(l_quantity) from hive.denormalized b where a.l_partkey = b.p_partkey );"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 17."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 18."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)  from hive.denormalized group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice  having sum(l_quantity) > 300  order by o_totalprice desc, o_orderdate;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 18."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 19."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select sum(l_extendedprice * (1 - l_discount) ) as revenue  from hive.denormalized where ( p_brand = 'Brand#12'and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')  and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG')  and l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#23'  and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10  and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG')  and l_shipinstruct = 'DELIVER IN PERSON' ) or (p_brand = 'Brand#34' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG')  and l_shipinstruct = 'DELIVER IN PERSON');"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 19."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 20."$i" ---------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 20."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 21."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select s_name, count(*) as numwait from hive.denormalized l1 where  o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from hive.denormalized l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from hive.denormalized l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n2_nationkey and n2_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 21."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 22."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/drill/apache-drill-1.15.0/bin/sqlline -u jdbc:drill:zk=zookeeper:2181 <<< "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(a.c_phone, 1,2) as cntrycode, a.c_acctbal from hive.denormalized a where  substring(a.c_phone, 1,2) in ('13','31','23','29','30','18','17') and a.c_acctbal > ( select avg(b.c_acctbal) from hive.denormalized b where b.c_acctbal > 0.00   and substring(b.c_phone, 1,2) in   ('13','31','23','29','30','18','17')) and not exists (select   *   from   hive.denormalized c   where   a.o_custkey = c.c_custkey   ) ) as custsale group by cntrycode order by cntrycode;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 22."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
done
else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. Paramter 2 for number of loops."
echo "Example ./imapalatest.sh 1 2"
exit 2
fi