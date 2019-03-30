#!/bin/bash

if [ ! -d $1 ] && [ ! -d $2 ]; then
mkdir -p results
cd results
resultPath=presto_normal_query_sf"$1"_result.txt
rm -f $resultPath
touch $resultPath
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------"|& tee -a $resultPath|& tee -a $resultPath
echo "-------------------------------------- "$resultPath" ---------------------------------"|& tee -a $resultPath|& tee -a $resultPath

echo "-------------------------------------- PRESTO QUERY NORMAL ---------------------------------"|& tee -a $resultPath|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo ""|& tee -a $resultPath

for i in $(seq 1 $2)
do
echo "--------------------------------------STARTED QUERY 1."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from l_lineitem where l_shipdate <= date '1998-12-01' - interval '90' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 1."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
d
echo "--------------------------------------STARTED QUERY 2."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from p_part, s_supplier, ps_partsupp, n_nation, r_region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select min(ps_supplycost) from ps_partsupp, s_supplier, n_nation, r_region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 2."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 3."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from c_customer, o_orders, l_lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 3."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 4."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select o_orderpriority, count(*) as order_count from o_orders where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists ( select * from l_lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 4."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 5."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from c_customer, o_orders, l_lineitem, s_supplier, n_nation, r_region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year group by n_name order by revenue desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 5."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 6."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select sum(l_extendedprice*l_discount) as revenue from l_lineitem where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 6."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 7."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from s_supplier, l_lineitem, o_orders, c_customer, n_nation n1, n_nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 7."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 8."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from p_part, s_supplier, l_lineitem, o_orders, c_customer, n_nation n1, n_nation n2, r_region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 8."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 9."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from p_part, s_supplier, l_lineitem, ps_partsupp, o_orders, n_nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 9."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 10."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from c_customer, o_orders, l_lineitem, n_nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 10."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 11."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select ps_partkey, sum(ps_supplycost * ps_availqty) as value1 from ps_partsupp, s_supplier, n_nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001 from ps_partsupp, s_supplier, n_nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' ) order by value1 desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 11."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 12."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from o_orders, l_lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 12."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 13."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from c_customer left outer join o_orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey )as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 13."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 14."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from l_lineitem, p_part where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 14."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 15."$i" ---------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 15."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 16."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from ps_partsupp, p_part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%'  and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from s_supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size limit 20000;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 16."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 17."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select sum(l_extendedprice) / 7.0 as avg_yearly from l_lineitem, p_part where p_partkey = l_partkey and  p_brand = 'Brand#23' and p_container = 'MED BOX'  and l_quantity < ( select 0.2 * avg(l_quantity) from l_lineitem where l_partkey = p_partkey );"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 17."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 18."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from c_customer, o_orders, l_lineitem where o_orderkey in ( select l_orderkey from l_lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 18."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 19."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select sum(l_extendedprice * (1 - l_discount) ) as revenue from l_lineitem, p_part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ); "|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 19."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 20."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select s_name, s_address from s_supplier, n_nation where s_suppkey in ( select ps_suppkey from ps_partsupp where ps_partkey in ( select p_partkey from p_part where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from l_lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date('1994-01-01') and l_shipdate < date('1994-01-01') + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name; "|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 20."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 21."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select s_name, count(*) as numwait from s_supplier, l_lineitem l1, o_orders, n_nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from l_lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from l_lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 21."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath

echo "--------------------------------------STARTED QUERY 22."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/usr/local/bin/presto --server coordinator:8080 --catalog hive --schema default "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from c_customer where substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17') and c_acctbal > ( select avg(c_acctbal) from c_customer where c_acctbal > 0.00 and substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17') ) and not exists ( select * from o_orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;  --Query 22 impala ok select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone, 1,2) as cntrycode, c_acctbal from c_customer where substring (c_phone,1,2) in ('13','31','23','29','30','18','17') and c_acctbal > ( select avg(c_acctbal) from c_customer where c_acctbal > 0.00 and substring (c_phone,1,2) in ('13','31','23','29','30','18','17') ) and not exists ( select * from o_orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 22."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
done
else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. Paramter 2 for number of loops."
echo "Example ./imapalatest.sh 1 2"
exit 2
fi