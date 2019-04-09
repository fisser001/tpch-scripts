#!/bin/bash

query1 () {
echo "--------------------------------------STARTED QUERY 1."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lo_lineitem_orders_star_parquet_impala where l_shipdate <= to_date(date_sub(cast('1998-12-01' as timestamp), interval 90 day)) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 1."$i"---------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query2 () {
echo "--------------------------------------STARTED  QUERY 2."$i" --------------------------------"|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 2."$i" --------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query3 () {
echo "--------------------------------------STARTED  QUERY 3."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, to_date(o_orderdate), o_shippriority from c_customer_star_parquet_impala inner join  lo_lineitem_orders_star_parquet_impala on c_custkey = o_custkey where c_mktsegment = 'BUILDING' and to_date(o_orderdate) < to_date('1995-03-15') and to_date(l_shipdate) > to_date('1995-03-15') group by l_orderkey, to_date(o_orderdate), o_shippriority order by revenue desc, to_date(o_orderdate) limit 10;" |& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 3."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query4 () {
echo "--------------------------------------STARTED  QUERY 4."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select o_orderpriority, count (distinct o_orderkey) as order_count  from lo_lineitem_orders_star_parquet_impala where o_orderdate >= to_date('1993-07-01') and to_date(o_orderdate) < to_date(date_add(cast('1993-07-01' as timestamp), interval 3 month)) and to_date(l_commitdate) < to_date(l_receiptdate ) group by o_orderpriority  order by o_orderpriority;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 4."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query5 () {
echo "--------------------------------------STARTED  QUERY 5."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select c.n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from c_customer_star_parquet_impala a , lo_lineitem_orders_star_parquet_impala b, s_supplier_star_parquet_impala c where a.c_custkey = b.o_custkey and b.l_suppkey = c.s_suppkey and c_nationkey = s_nationkey and c.r_name = 'ASIA' and to_date(b.o_orderdate) >= to_date('1994-01-01') and to_date(b.o_orderdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year)) group by c.n_name order by revenue desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 5."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query6 () {
echo "--------------------------------------STARTED  QUERY 6."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select sum(l_extendedprice*l_discount) as revenue from lo_lineitem_orders_star_parquet_impala where to_date(l_shipdate) >= to_date('1994-01-01') and to_date(l_shipdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year)) and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 6."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query7 () {
echo "--------------------------------------STARTED  QUERY 7."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select a.n_name as supp_nation, c.n_name as cust_nation, year(to_date(l_shipdate)) as l_year, b.l_extendedprice * (1 - b.l_discount) as volume from s_supplier_star_parquet_impala a, lo_lineitem_orders_star_parquet_impala b, c_customer_star_parquet_impala c where a.s_suppkey = b.l_suppkey and c.c_custkey = b.o_custkey and ( (a.n_name = 'FRANCE' and c.n_name = 'GERMANY') or (a.n_name = 'GERMANY' and c.n_name = 'FRANCE') ) and to_date(l_shipdate) between to_date('1995-01-01') and to_date('1996-12-31') ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 7."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query8 () {
echo "--------------------------------------STARTED  QUERY 8."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select year(to_date(o_orderdate)) as o_year, l_extendedprice * (1-l_discount) as volume, b.n_name as nation from p_part_star_parquet_impala a, s_supplier_star_parquet_impala b, lo_lineitem_orders_star_parquet_impala c, c_customer_star_parquet_impala d where a.p_partkey = c.l_partkey and b.s_suppkey = c.l_suppkey and c.o_custkey = d.c_custkey and d.r_name = 'AMERICA' and c.o_orderdate between to_date('1995-01-01') and to_date('1996-12-31') and a.p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 8."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query9 () {
echo "--------------------------------------STARTED  QUERY 9."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select nation, o_year, sum(amount) as sum_profit from ( select b.n_name as nation, year(to_date(c.o_orderdate)) as o_year, c.l_extendedprice * (1 - c.l_discount) - d.ps_supplycost * c.l_quantity as amount from p_part_star_parquet_impala a, s_supplier_star_parquet_impala b, lo_lineitem_orders_star_parquet_impala c, ps_partsupp_star_parquet_impala d where b.s_suppkey = c.l_suppkey and d.ps_suppkey = c.l_suppkey and d.ps_partkey = c.l_partkey and a.p_partkey = c.l_partkey and a.p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 9."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query10 () {
echo "--------------------------------------STARTED  QUERY 10."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select a.c_custkey, a.c_name, sum(b.l_extendedprice * (1 - b.l_discount)) as revenue, a.c_acctbal, a.n_name, a.c_address, a.c_phone, a.c_comment from c_customer_star_parquet_impala a, lo_lineitem_orders_star_parquet_impala b where a.c_custkey = b.o_custkey and b.o_orderdate >= to_date('1993-10-01') and b.o_orderdate < to_date(date_add(cast('1993-10-01' as timestamp), interval 3 month)) and b.l_returnflag = 'R' group by a.c_custkey, a.c_name, a.c_acctbal, a.c_phone, a.n_name, a.c_address, a.c_comment order by revenue desc limit 20;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 10."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query11 () {
echo "--------------------------------------STARTED  QUERY 11."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "create view q11_part_tmp_cached as select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value from ps_partsupp_star_parquet_impala,s_supplier_star_parquet_impala where ps_suppkey = s_suppkey and n_name = 'GERMANY' group by ps_partkey;  create view q11_sum_tmp_cached as select sum(part_value) as total_value from q11_part_tmp_cached;  select ps_partkey, part_value as value from (select ps_partkey,part_value,total_value  from q11_part_tmp_cached join q11_sum_tmp_cached ) a where part_value > total_value * 0.0001 order by value desc;  drop view q11_part_tmp_cached; drop view q11_sum_tmp_cached;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 11."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query12 () {
echo "--------------------------------------STARTED  QUERY 12."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from lo_lineitem_orders_star_parquet_impala where l_shipmode in ('MAIL', 'SHIP') and to_date(l_commitdate) < to_date(l_receiptdate) and l_shipdate < l_commitdate and to_date(l_receiptdate) >= to_date('1994-01-01') and to_date(l_receiptdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year)) group by l_shipmode order by l_shipmode;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 12."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query13 () {
echo "--------------------------------------STARTED  QUERY 13."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select c_count, count(*) as custdist from ( select c_custkey, count(distinct o_orderkey) as c_count from c_customer_star_parquet_impala left outer join lo_lineitem_orders_star_parquet_impala on c_custkey = o_custkey and o_orderkey is not null and o_comment not like '%special%requests%' group by c_custkey )as c_orders group by c_count order by custdist desc, c_count desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 13."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query14 () {
echo "--------------------------------------STARTED  QUERY 14."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lo_lineitem_orders_star_parquet_impala, p_part_star_parquet_impala where l_partkey = p_partkey and to_date(l_shipdate) >= to_date('1995-09-01') and to_date(l_shipdate) < to_date(date_add(cast('1995-09-01' as timestamp), interval 1 month));"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 14."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query15 () {
echo "--------------------------------------STARTED  QUERY 15."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "create view v_revenue_star (supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lo_lineitem_orders_star_parquet_impala where to_date(l_shipdate) >= to_date('1996-01-01') and l_shipdate < to_date(date_add(cast('1996-01-01' as timestamp), interval 3 month)) group by l_suppkey;  select s_suppkey, s_name, s_address, s_phone, total_revenue from s_supplier_star_parquet_impala, v_revenue_star where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from v_revenue_star ) order by s_suppkey;  drop view v_revenue_star;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 15."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query16 () {
echo "--------------------------------------STARTED  QUERY 16."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from ps_partsupp_star_parquet_impala, p_part_star_parquet_impala where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%'  and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from s_supplier_star_parquet_impala where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size limit 20000;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 16."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query17 () {
echo "--------------------------------------STARTED  QUERY 17."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select sum(l_extendedprice) / 7.0 as avg_yearly from lo_lineitem_orders_star_parquet_impala, p_part_star_parquet_impala where p_partkey = l_partkey and  p_brand = 'Brand#23' and p_container = 'MED BOX'  and l_quantity < ( select 0.2 * avg(l_quantity) from lo_lineitem_orders_star_parquet_impala where l_partkey = p_partkey ); "|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 17."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query18 () {
echo "--------------------------------------STARTED  QUERY 18."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from c_customer_star_parquet_impala, lo_lineitem_orders_star_parquet_impala where o_orderkey in ( select l_orderkey from lo_lineitem_orders_star_parquet_impala group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 18."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query19 () {
echo "--------------------------------------STARTED  QUERY 19."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select sum(l_extendedprice * (1 - l_discount) ) as revenue from lo_lineitem_orders_star_parquet_impala, p_part_star_parquet_impala where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ); "|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 19."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query20 () {
echo "--------------------------------------STARTED  QUERY 20."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select s_name, s_address from s_supplier_star_parquet_impala where s_suppkey in ( select ps_suppkey from ps_partsupp_star_parquet_impala where ps_partkey in ( select p_partkey from p_part_star_parquet_impala where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lo_lineitem_orders_star_parquet_impala where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= to_date('1994-01-01') and l_shipdate < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year)) ) ) and n_name = 'CANADA' order by s_name;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 20."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query21 () {
echo "--------------------------------------STARTED  QUERY 21."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select s_name, count(*) as numwait from s_supplier_star_parquet_impala, lo_lineitem_orders_star_parquet_impala l1 where s_suppkey = l1.l_suppkey and  l1.o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lo_lineitem_orders_star_parquet_impala l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lo_lineitem_orders_star_parquet_impala l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and  n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 21."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query22 () {
echo "--------------------------------------STARTED  QUERY 22."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
impala-shell -q "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone , 1 , 2) as cntrycode, c_acctbal from c_customer_star_parquet_impala where substring (c_phone , 1 , 2) in ('13','31','23','29','30','18','17') and c_acctbal > ( select avg(c_acctbal) from c_customer_star_parquet_impala where c_acctbal > 0.00 and substring (c_phone , 1 , 2) in ('13','31','23','29','30','18','17') ) and not exists ( select * from lo_lineitem_orders_star_parquet_impala where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 22."$i"----------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
}

if [ ! -d $1 ] && [ ! -d $2 ] && [ ! -d $3 ]; then
cd /data/mydata
resultPath=impala_star_query_sf"$1"_"$3"_result.txt
rm -f $resultPath
touch $resultPath
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------" |& tee -a $resultPath
echo "-------------------------------------- "$resultPath" ---------------------------------"|& tee -a $resultPath

echo "-------------------------------------- IMPALA QUERY STAR ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo ""|& tee -a $resultPath


if [ $3 = "single" ]; then
array=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 )
echo "-------------------------------------- STARTED TO INVALIDATE METADATA ---------------------------------"|& tee -a $resultPath
impala-shell -q "INVALIDATE METADATA;"
echo "-------------------------------------- ENDED TO INVALIDATE METADATA ---------------------------------"|& tee -a $resultPath

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
    for m in "${array[@]}"
    do
        query"$m"
    done

done
else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. 
Paramter 2 for number of loops. Parameter 3 for mode which can be single or multiple1 or multiple2 or multiple3 or multiple4"
echo "Example ./hive_mr_normal_query.sh 1 2 single"
exit 2
fi