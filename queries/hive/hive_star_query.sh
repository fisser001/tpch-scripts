#!/bin/bash

query1 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lo_lineitem_orders_star where l_shipdate <= date '1998-12-01' - interval '90' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query2 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query3 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from c_customer_star inner join  lo_lineitem_orders_star on c_custkey = o_custkey where c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query4 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select o_orderpriority, count (distinct o_orderkey) as order_count  from lo_lineitem_orders_star where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month  and l_commitdate < l_receiptdate  group by o_orderpriority  order by o_orderpriority;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query5 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select c.n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from c_customer_star a , lo_lineitem_orders_star b, s_supplier_star c where a.c_custkey = b.o_custkey and b.l_suppkey = c.s_suppkey and c_nationkey = s_nationkey and c.r_name = 'ASIA' and b.o_orderdate >= date '1994-01-01' and b.o_orderdate < date '1994-01-01' + interval '1' year group by c.n_name order by revenue desc;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query6 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select c.n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from c_customer_star a , lo_lineitem_orders_star b, s_supplier_star c where a.c_custkey = b.o_custkey and b.l_suppkey = c.s_suppkey and c_nationkey = s_nationkey and c.r_name = 'ASIA' and b.o_orderdate >= date '1994-01-01' and b.o_orderdate < date '1994-01-01' + interval '1' year group by c.n_name order by revenue desc;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query7 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select a.n_name as supp_nation, c.n_name as cust_nation, extract(year from b.l_shipdate) as l_year, b.l_extendedprice * (1 - b.l_discount) as volume from s_supplier_star a, lo_lineitem_orders_star b, c_customer_star c where a.s_suppkey = b.l_suppkey and c.c_custkey = b.o_custkey and ( (a.n_name = 'FRANCE' and c.n_name = 'GERMANY') or (a.n_name = 'GERMANY' and c.n_name = 'FRANCE') ) and b.l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query8 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; set hive.strict.checks.cartesian.product=false; select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, b.n_name as nation from p_part_star a, s_supplier_star b, lo_lineitem_orders_star c, c_customer_star d where a.p_partkey = c.l_partkey and b.s_suppkey = c.l_suppkey and c.o_custkey = d.c_custkey and d.r_name = 'AMERICA' and c.o_orderdate between date '1995-01-01' and date '1996-12-31' and a.p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}


query9 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; set hive.auto.convert.join=false; select nation, o_year, sum(amount) as sum_profit from ( select b.n_name as nation, extract(year from c.o_orderdate) as o_year, c.l_extendedprice * (1 - c.l_discount) - d.ps_supplycost * c.l_quantity as amount from p_part_star a inner join  lo_lineitem_orders_star c  on a.p_partkey = c.l_partkey  inner join  s_supplier_star b  on b.s_suppkey = c.l_suppkey inner join  ps_partsupp_star d on  d.ps_suppkey = c.l_suppkey and d.ps_partkey = c.l_partkey where a.p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc; set hive.auto.convert.join=true;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query10 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select a.c_custkey, a.c_name, sum(b.l_extendedprice * (1 - b.l_discount)) as revenue, a.c_acctbal, a.n_name, a.c_address, a.c_phone, a.c_comment from c_customer_star a, lo_lineitem_orders_star b where a.c_custkey = b.o_custkey and b.o_orderdate >= date '1993-10-01' and b.o_orderdate < date '1993-10-01' + interval '3' month and b.l_returnflag = 'R' group by a.c_custkey, a.c_name, a.c_acctbal, a.c_phone, a.n_name, a.c_address, a.c_comment order by revenue desc limit 20; "
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query11 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; drop view q11_part_tmp_cached; drop view q11_sum_tmp_cached; create view q11_part_tmp_cached as select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value from ps_partsupp_star,s_supplier_star where ps_suppkey = s_suppkey and n_name = 'GERMANY' group by ps_partkey;  create view q11_sum_tmp_cached as select sum(part_value) as total_value from q11_part_tmp_cached; set hive.strict.checks.cartesian.product=false; select ps_partkey, part_value as value from (select ps_partkey,part_value,total_value  from q11_part_tmp_cached inner join q11_sum_tmp_cached ) a where part_value > total_value * 0.0001 order by value desc; set hive.strict.checks.cartesian.product=true; drop view q11_part_tmp_cached; drop view q11_sum_tmp_cached;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query12 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from lo_lineitem_orders_star where l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query13 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select c_count, count(*) as custdist from ( select c_custkey, count(distinct o_orderkey) as c_count from c_customer_star left outer join lo_lineitem_orders_star on c_custkey = o_custkey and o_orderkey is not null and o_comment not like '%special%requests%' group by c_custkey )as c_orders group by c_count order by custdist desc, c_count desc;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query14 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lo_lineitem_orders_star, p_part_star where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query15 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; create view revenue_cached as select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue from lo_lineitem_orders_star where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01' group by l_suppkey; create view max_revenue_cached as select max(total_revenue) as max_revenue from revenue_cached; select s_suppkey, s_name,s_address,s_phone, total_revenue from s_supplier_star, revenue_cached, max_revenue_cached where s_suppkey = supplier_no and total_revenue = max_revenue  order by s_suppkey; drop view revenue_cached; drop view max_revenue_cached;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query16 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; set hive.strict.checks.cartesian.product=false; select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from ps_partsupp_star inner join  p_part_star on  p_partkey = ps_partkey where p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%'  and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from s_supplier_star where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size limit 20000; set hive.strict.checks.cartesian.product=true;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query17 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select sum(l_extendedprice) / 7.0 as avg_yearly from lo_lineitem_orders_star, p_part_star where p_partkey = l_partkey and  p_brand = 'Brand#23' and p_container = 'MED BOX'  and l_quantity < ( select 0.2 * avg(l_quantity) from lo_lineitem_orders_star where l_partkey = p_partkey );"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query18 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from c_customer_star, lo_lineitem_orders_star where o_orderkey in ( select l_orderkey from lo_lineitem_orders_star group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query19 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select sum(l_extendedprice * (1 - l_discount) ) as revenue from lo_lineitem_orders_star, p_part_star where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ); "
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query20 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 set hive.execution.engine=$engine; set mapred.job.queue.name=$queuename; set tez.queue.name=$queuename;  SET mapreduce.framework.name=$framework; select s_name, s_address from s_supplier_star where s_suppkey in ( select ps_suppkey from ps_partsupp_star where ps_partkey in ( select p_partkey from p_part_star where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lo_lineitem_orders_star where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date('1994-01-01') and l_shipdate < date('1994-01-01') + interval '1' year ) ) and n_name = 'CANADA' order by s_name;"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query21 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

query22 () {
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 select from_unixtime(unix_timestamp(current_timestamp)+7200);"
test1="$test1 !sh echo '--------------------------------------Finished QUERY $m.$i--------------------------------';"
}

if [ ! -d $1 ] && [ ! -d $2 ] && [ ! -d $3 ] && [ ! -d $4 ] && [ ! -d $5 ] && [ ! -d $6 ]; then
cd /data/mydata
resultPath=hive_star_query_sf"$1"_"$3"_"$5"_"$6"_result.txt
rm -f $resultPath
touch $resultPath
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------" |& tee -a $resultPath
echo "-------------------------------------- "$resultPath" ---------------------------------"|& tee -a $resultPath

echo "-------------------------------------- HIVE QUERY STAR ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo ""|& tee -a $resultPath


if [ $3 = "single" ]; then
array=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 )
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


queuename=$4
engine=$5
framework=$6
test1=""
for i in $(seq 1 $2)
do
    for m in "${array[@]}"
    do
        query"$m"
    done
done
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set tez.am.container.reuse.enabled=true; $test1"|& tee -a $resultPath
else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. 
Paramter 2 for number of loops. Parameter 3 for mode which can be single or multiple1 or multiple2 or multiple3 or multiple4
Parameter 4 is for queuename. Parameter 5 is for execution engine. 
Parameter 6 is for local or yarn."
echo "Example ./hive_mr_normal_query.sh 1 2 single default tez yarn"
exit 2
fi