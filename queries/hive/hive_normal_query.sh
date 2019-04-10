#!/bin/bash

query1 () {
test1="$test1 select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:MM:SS.sss');"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,  avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc, count(*) as count_order  from l_lineitem  where l_shipdate <= date '1998-12-01' - interval '90' day  group by l_returnflag, l_linestatus  order by l_returnflag, l_linestatus;"
test1="$test1 select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:MM:SS.sss');"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query2 () {
test1="$test1 select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:MM:SS.sss');"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,  avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc, count(*) as count_order  from l_lineitem  where l_shipdate <= date '1998-12-01' - interval '90' day  group by l_returnflag, l_linestatus  order by l_returnflag, l_linestatus;"
test1="$test1 select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:MM:SS.sss');"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query222 () {
!sh echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
!sh echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
#/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e 
"set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; set hive.auto.convert.join=false; select s_acctbal,s_name, n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment from p_part inner join ps_partsupp on p_partkey = ps_partkey and p_size = 15 and p_type like '%BRASS' inner join s_supplier on s_suppkey = ps_suppkey inner join n_nation on s_nationkey = n_nationkey inner join r_region on n_regionkey = r_regionkey and r_name = 'EUROPE' where ps_supplycost = (select min(ps_supplycost) from ps_partsupp, s_supplier, n_nation, r_region where p_partkey = ps_partkey and  s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE')  order by s_acctbal desc,n_name,s_name,p_partkey limit 100; set hive.auto.convert.join=true;"|& tee -a $resultPath
!sh echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
!sh echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
!sh echo ""|& tee -a $resultPath
}

query3 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from c_customer, o_orders, l_lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query4 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select o_orderpriority, count(*) as order_count from o_orders where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists (select * from l_lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query5 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue from c_customer, o_orders, l_lineitem, s_supplier, n_nation, r_region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year group by n_name order by revenue desc; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query6 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select sum(l_extendedprice * l_discount) as revenue from l_lineitem where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query7 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; select supp_nation, cust_nation,l_year, sum(volume) as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, (l_extendedprice * (1 - l_discount)) as volume from s_supplier inner join l_lineitem on s_suppkey = l_suppkey inner join o_orders on o_orderkey = l_orderkey inner join c_customer on c_custkey = o_custkey inner join n_nation n1 on s_nationkey = n1.n_nationkey inner join n_nation n2 on c_nationkey = n2.n_nationkey where ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping group by supp_nation,cust_nation,l_year order by supp_nation, cust_nation, l_year; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query8 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from p_part inner join l_lineitem on p_partkey = l_partkey inner join s_supplier on s_suppkey = l_suppkey inner join o_orders on l_orderkey = o_orderkey inner join c_customer on o_custkey = c_custkey inner join n_nation n1 on c_nationkey = n1.n_nationkey inner join n_nation n2 on s_nationkey = n2.n_nationkey inner join r_region on n1.n_regionkey = r_regionkey where r_name = 'AMERICA' and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query9 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; select nation, o_year, sum(amount) as sum_profit from (select n_name as nation, extract(year from o_orderdate) as o_year,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from p_part inner join l_lineitem on p_partkey = l_partkey inner join s_supplier on s_suppkey = l_suppkey inner join ps_partsupp on ps_suppkey = l_suppkey and ps_partkey = l_partkey inner join o_orders on o_orderkey = l_orderkey inner join n_nation on s_nationkey = n_nationkey where p_name like '%green%') as profit group by nation, o_year order by nation, o_year desc; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query10 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e " set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment from c_customer, o_orders, l_lineitem, n_nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal,c_phone,n_name,c_address, c_comment order by revenue desc limit 20; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query11 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  create view q11_part_tmp_cached as select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value from ps_partsupp,s_supplier,n_nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey; create view q11_sum_tmp_cached as select sum(part_value) as total_value from q11_part_tmp_cached; set hive.strict.checks.cartesian.product=false; select ps_partkey, part_value as value from (select ps_partkey,part_value,total_value from q11_part_tmp_cached join q11_sum_tmp_cached) a where part_value > total_value * 0.0001 order by value desc; set hive.strict.checks.cartesian.product=true; drop view q11_sum_tmp_cached;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query12 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from o_orders, l_lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query13 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select c_count, count(*) as custdist from (select c_custkey,count(o_orderkey) as c_count from c_customer left outer join o_orders on c_custkey = o_custkey and o_comment not like '%special%requests%'group by c_custkey) as c_orders group by c_count order by custdist desc, c_count desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query14 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from l_lineitem, p_part where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query15 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; create view revenue_cached as select 	l_suppkey as supplier_no, 	sum(l_extendedprice * (1 - l_discount)) as total_revenue from 	l_lineitem where 	l_shipdate >= '1996-01-01' 	and l_shipdate < '1996-04-01' group by l_suppkey;  create view max_revenue_cached as select 	max(total_revenue) as max_revenue from 	revenue_cached;  select 	s_suppkey, 	s_name, 	s_address, 	s_phone, 	total_revenue from 	s_supplier, 	revenue_cached, 	max_revenue_cached where 	s_suppkey = supplier_no 	and total_revenue = max_revenue  order by s_suppkey; drop view revenue_cached; drop view max_revenue_cached;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query16 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select p_brand, p_type,p_size, count(distinct ps_suppkey) as supplier_cnt from ps_partsupp inner join p_part on p_partkey = ps_partkey left outer join s_supplier on ps_suppkey = s_suppkey where s_comment not like '%Customer%Complaints%' and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) group by p_brand,p_type,p_size order by supplier_cnt desc, p_brand,p_type, p_size limit 20000;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query17 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select sum(l_extendedprice) / 7.0 as avg_yearly from l_lineitem, p_part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity) from l_lineitem where l_partkey = p_partkey);"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query18 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select c_name,c_custkey,o_orderkey, o_orderdate,o_totalprice,sum(l_quantity) from c_customer, o_orders, l_lineitem where o_orderkey in (select l_orderkey from l_lineitem group by l_orderkey having sum(l_quantity) > 300) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name,c_custkey, o_orderkey, o_orderdate,o_totalprice order by o_totalprice desc, o_orderdate limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query19 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select sum(l_extendedprice* (1 - l_discount)) as revenue from l_lineitem, p_part where (p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON');"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query20 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  select s_name,s_address from s_supplier, n_nation where s_suppkey in (select ps_suppkey from ps_partsupp where ps_partkey in (select p_partkey from p_part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from l_lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year)) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query21 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  create temporary table l3 stored as orc as select l_orderkey, count(distinct l_suppkey) as cntSupp from l_lineitem where l_receiptdate > l_commitdate and l_orderkey is not null group by l_orderkey having cntSupp = 1; set hive.auto.convert.join=false; with location as (select s_supplier.* from s_supplier, n_nation where s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA') select s_name, count(*) as numwait from (select li.l_suppkey, li.l_orderkey from l_lineitem li join o_orders o on li.l_orderkey = o.o_orderkey and o.o_orderstatus = 'F' join ( select l_orderkey, count(distinct l_suppkey) as cntSupp     from l_lineitem     group by l_orderkey     ) l2 on li.l_orderkey = l2.l_orderkey and              li.l_receiptdate > li.l_commitdate and              l2.cntSupp > 1) l1 join l3 on l1.l_orderkey = l3.l_orderkey join location s on l1.l_suppkey = s.s_suppkey group by s_name order by numwait desc, s_name limit 100; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query22 () {
echo "--------------------------------------STARTED QUERY "$m"."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework;  set hive.auto.convert.join=false; create view if not exists q22_customer_tmp_cached as select c_acctbal, c_custkey,substr(c_phone, 1, 2) as cntrycode from c_customer where substr(c_phone, 1, 2) = '13' or substr(c_phone, 1, 2) = '31' or substr(c_phone, 1, 2) = '23' or substr(c_phone, 1, 2) = '29' or substr(c_phone, 1, 2) = '30' or substr(c_phone, 1, 2) = '18' or substr(c_phone, 1, 2) = '17'; create view if not exists q22_customer_tmp1_cached as select avg(c_acctbal) as avg_acctbal from q22_customer_tmp_cached where c_acctbal > 0.00; create view if not exists q22_orders_tmp_cached as select o_custkey from o_orders group by o_custkey; set hive.strict.checks.cartesian.product=false; select cntrycode, count(1) as numcust, sum(c_acctbal) as totacctbal from (    select cntrycode, c_acctbal,avg_acctbal from q22_customer_tmp1_cached ct1 inner join (select cntrycode, c_acctbal from q22_orders_tmp_cached ot right outer join q22_customer_tmp_cached ct on ct.c_custkey = ot.o_custkey where o_custkey is null) ct2) a where c_acctbal > avg_acctbal group by cntrycode order by cntrycode; set hive.strict.checks.cartesian.product=true; drop view q22_customer_tmp1_cached; drop view q22_orders_tmp_cached; set hive.auto.convert.join=false;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY "$m"."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

if [ ! -d $1 ] && [ ! -d $2 ] && [ ! -d $3 ] && [ ! -d $4 ] && [ ! -d $5 ] && [ ! -d $6 ]; then
cd /data/mydata
resultPath=hive_normal_query_sf"$1"_"$3"_"$5"_"$6"_result.txt
rm -f $resultPath
touch $resultPath
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------" |& tee -a $resultPath
echo "-------------------------------------- "$resultPath" ---------------------------------"|& tee -a $resultPath

echo "-------------------------------------- HIVE QUERY NORMALIZED ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo ""|& tee -a $resultPath


if [ $3 = "single" ]; then
array=( 1 2 ) #3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 )
fi

if [ $3 = "multiple1" ]; then
array=( 21 3 18 5 11 7 6 20 17 12 16 15 13 10 2 8 14 19 9 22 1 4)
fi

if [ $3 = "multiple2" ]; then
array=(2 6 17 14 16 19 10 9 2 15 8 5 22 12 7 13 18 1 4 20 3 11 21 )
fi

if [ $3 = "multiple3" ]; then
array=(3 8 5 4 6 17 7 1 18 22 14 9 10 15 11 20 2 21 19 13 16 12 3)
fi

if [ $3 = "multiple4" ]; then
array=(4 5 21 14 19 15 17 12 6 4 9 8 16 11 2 10 18 1 13 7 22 3 20)
fi


for i in $(seq 1 $2)
do
queuename=$4
engine=$5
framework=$6
test1=""
#/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e
    for m in "${array[@]}"
    do
        query"$m"
       # echo $test1
    done
    /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "$test1"|& tee -a $resultPath
done
else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. 
Paramter 2 for number of loops. Parameter 3 for mode which can be single or multiple1 or multiple2 or multiple3 or multiple4
Parameter 4 is for queuename. Parameter 5 is for execution engine. 
Parameter 6 is for local or yarn."
echo "Example ./hive_mr_normal_query.sh 1 2 single default tez yarn"
exit 2
fi

#/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e 'show tables; !sh echo "STARED QUERY 1.1";'
