#!/bin/bash

query1 () {
echo "--------------------------------------STARTED QUERY 1."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,  avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc, count(*) as count_order  from l_lineitem  where l_shipdate <= date '1998-12-01' - interval '90' day  group by l_returnflag, l_linestatus  order by l_returnflag, l_linestatus;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 1."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query2 () {
echo "--------------------------------------STARTED QUERY 2."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select s_acctbal,s_name, n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment from p_part inner join ps_partsupp on p_partkey = ps_partkey and p_size = 15 and p_type like '%BRASS' inner join s_supplier on s_suppkey = ps_suppkey inner join n_nation on s_nationkey = n_nationkey inner join r_region on n_regionkey = r_regionkey and r_name = 'EUROPE' where ps_supplycost = (select min(ps_supplycost) from ps_partsupp, s_supplier, n_nation, r_region where p_partkey = ps_partkey and  s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE')  order by s_acctbal desc,n_name,s_name,p_partkey limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 2."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query3 () {
echo "--------------------------------------STARTED QUERY 3."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; set hive.auto.convert.join=false; select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from c_customer, o_orders, l_lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 3."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query4 () {
echo "--------------------------------------STARTED QUERY 4"$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select o_orderpriority, count(*) as order_countfrom o_orderswhere o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists (select * from l_lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)group by o_orderpriority order by o_orderpriority;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 4."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query5 () {
echo "--------------------------------------STARTED QUERY 5."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; set hive.auto.convert.join=false; select n_name,sum(l_extendedprice * (1 - l_discount)) as revenuefrom c_customer, o_orders, l_lineitem, s_supplier, n_nation, r_regionwhere c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1994-01-01'and o_orderdate < date '1994-01-01' + interval '1' yeargroup by n_nameorder by revenue desc; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 5."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query6 () {
echo "--------------------------------------STARTED QUERY 6."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select sum(l_extendedprice * l_discount) as revenuefrom l_lineitemwhere l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 6."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query7 () {
echo "--------------------------------------STARTED QUERY 7."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; set hive.auto.convert.join=false; select supp_nation, cust_nation,l_year, sum(volume) as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, (l_extendedprice * (1 - l_discount)) as volume from s_supplier inner join l_lineitemon s_suppkey = l_suppkeyinner join o_orderson o_orderkey = l_orderkeyinner join c_customeron c_custkey = o_custkeyinner join n_nation n1on s_nationkey = n1.n_nationkeyinner join n_nation n2 on c_nationkey = n2.n_nationkeywhere ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping group by supp_nation,cust_nation,l_year order by supp_nation, cust_nation, l_year; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 7."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query8 () {
echo "--------------------------------------STARTED QUERY 8."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select o_year, sum(case	when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_sharefrom (select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nationfrom p_part inner join l_lineitemon p_partkey = l_partkeyinner join s_supplieron s_suppkey = l_suppkeyinner join o_orderson l_orderkey = o_orderkeyinner join c_customeron o_custkey = c_custkeyinner join n_nation n1on c_nationkey = n1.n_nationkeyinner join n_nation n2on s_nationkey = n2.n_nationkeyinner join r_regionon n1.n_regionkey = r_regionkeywhere  r_name = 'AMERICA' and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nationsgroup by o_yearorder by o_year;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 8."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query9 () {
echo "--------------------------------------STARTED QUERY 9."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; set hive.auto.convert.join=false; select nation, o_year, sum(amount) as sum_profitfrom (select n_name as nation, extract(year from o_orderdate) as o_year,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amountfrom p_part inner join l_lineitemon p_partkey = l_partkeyinner join s_supplieron s_suppkey = l_suppkeyinner join ps_partsuppon ps_suppkey = l_suppkey and ps_partkey = l_partkey inner join o_orderson o_orderkey = l_orderkeyinner join n_nationon s_nationkey = n_nationkeywhere p_name like '%green%') as profitgroup by nation, o_year order by nation, o_year desc; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 9."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query10 () {
echo "--------------------------------------STARTED QUERY 10."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e " set hive.execution.engine=mr; select set hive.auto.convert.join=false; select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_commentfrom c_customer, o_orders, l_lineitem, n_nationwhere c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkeygroup by c_custkey, c_name, c_acctbal,c_phone,n_name,c_address, c_commentorder by revenue desc limit 20; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 10."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query11 () {
echo "--------------------------------------STARTED QUERY 11."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select create view q11_part_tmp_cached asselect ps_partkey,sum(ps_supplycost * ps_availqty) as part_valuefrom ps_partsupp,s_supplier,n_nationwhere ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'group by ps_partkey; create view q11_sum_tmp_cached asselect sum(part_value) as total_valuefrom q11_part_tmp_cached; set hive.strict.checks.cartesian.product=false; select ps_partkey, part_value as valuefrom (select ps_partkey,part_value,total_value from q11_part_tmp_cached join q11_sum_tmp_cached) awhere part_value > total_value * 0.0001order byvalue desc; set hive.strict.checks.cartesian.product=true; drop view q11_sum_tmp_cached;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 11."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query12 () {
echo "--------------------------------------STARTED QUERY 12."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; set hive.auto.convert.join=false; select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_countfrom o_orders, l_lineitemwhere o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdateand l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' yeargroup by l_shipmodeorder by l_shipmode; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 12."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query13 () {
echo "--------------------------------------STARTED QUERY 13."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select c_count, count(*) as custdist from (select c_custkey,count(o_orderkey) as c_countfrom c_customer left outer join o_orders on c_custkey = o_custkeyand o_comment not like '%special%requests%'group by c_custkey) as c_ordersgroup by c_countorder by custdist desc, c_count desc;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 13."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query14 () {
echo "--------------------------------------STARTED QUERY 14."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenuefrom l_lineitem, p_part where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 14."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query15 () {
echo "--------------------------------------STARTED QUERY 15."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; create view v_revenue (supplier_no, total_revenue) asselect l_suppkey,sum(l_extendedprice * (1 - l_discount))from denormalized	where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' monthgroup by l_suppkey; select s_suppkey,s_name,s_address,s_phone,max(total_revenue) as total_revenuefrom denormalized, v_revenue where s_suppkey = supplier_nogroup by s_suppkey,s_name,s_address,s_phoneorder by total_revenue desc limit 1; drop view v_revenue;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 15."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query16 () {
echo "--------------------------------------STARTED QUERY 16."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select p_brand, p_type,p_size, count(distinct ps_suppkey) as supplier_cntfrom ps_partsupp inner join p_part on p_partkey = ps_partkey left outer join s_supplieron ps_suppkey = s_suppkeywhere s_comment not like '%Customer%Complaints%'and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9)group by p_brand,p_type,p_sizeorder by supplier_cnt desc, p_brand,p_type, p_size limit 20000;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 16."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query17 () {
echo "--------------------------------------STARTED QUERY 17."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select sum(l_extendedprice) / 7.0 as avg_yearlyfrom l_lineitem, p_partwhere p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity)from l_lineitemwhere l_partkey = p_partkey);"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 17."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query18 () {
echo "--------------------------------------STARTED QUERY 18."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select c_name,c_custkey,o_orderkey, o_orderdate,o_totalprice,sum(l_quantity) from c_customer, o_orders, l_lineitem where o_orderkey in (select l_orderkey from l_lineitemgroup by l_orderkey having sum(l_quantity) > 300)and c_custkey = o_custkeyand o_orderkey = l_orderkeygroup by c_name,c_custkey, o_orderkey, o_orderdate,o_totalpriceorder by o_totalprice desc, o_orderdate limit 100;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 18."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query19 () {
echo "--------------------------------------STARTED QUERY 19."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select sum(l_extendedprice* (1 - l_discount)) as revenuefrom l_lineitem, p_partwhere (p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')and l_quantity >= 1 and l_quantity <= 1 + 10and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG')and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON');"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 19."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query20 () {
echo "--------------------------------------STARTED QUERY 20."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select s_name,s_address from s_supplier, n_nation where s_suppkey in (select ps_suppkey from ps_partsuppwhere ps_partkey in (select p_partkey from p_partwhere p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from l_lineitemwhere l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year))and s_nationkey = n_nationkey and n_name = 'CANADA'order by s_name;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 20."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query21 () {
echo "--------------------------------------STARTED QUERY 21."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; create temporary table l3 stored as orc as select l_orderkey, count(distinct l_suppkey) as cntSuppfrom l_lineitemwhere l_receiptdate > l_commitdate and l_orderkey is not nullgroup by l_orderkeyhaving cntSupp = 1; set hive.auto.convert.join=false; with location as (select s_supplier.* from s_supplier, n_nation wheres_nationkey = n_nationkey and n_name = 'SAUDI ARABIA')select s_name, count(*) as numwaitfrom(select li.l_suppkey, li.l_orderkeyfrom l_lineitem li join o_orders o on li.l_orderkey = o.o_orderkey and                      o.o_orderstatus = 'F'     join     (     select l_orderkey, count(distinct l_suppkey) as cntSupp     from l_lineitem     group by l_orderkey     ) l2 on li.l_orderkey = l2.l_orderkey and              li.l_receiptdate > li.l_commitdate and              l2.cntSupp > 1) l1 join l3 on l1.l_orderkey = l3.l_orderkey join location s on l1.l_suppkey = s.s_suppkeygroup by s_nameorder by numwait desc, s_namelimit 100; set hive.auto.convert.join=true;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 21."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

query22 () {
echo "--------------------------------------STARTED QUERY 22."$i" ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; create view if not exists q22_customer_tmp_cached asselect c_acctbal, c_custkey,substr(c_phone, 1, 2) as cntrycodefrom c_customerwhere substr(c_phone, 1, 2) = '13' or substr(c_phone, 1, 2) = '31' or substr(c_phone, 1, 2) = '23' orsubstr(c_phone, 1, 2) = '29' or substr(c_phone, 1, 2) = '30' or substr(c_phone, 1, 2) = '18' or substr(c_phone, 1, 2) = '17'; create view if not exists q22_customer_tmp1_cached asselect avg(c_acctbal) as avg_acctbal from q22_customer_tmp_cachedwhere c_acctbal > 0.00; create view if not exists q22_orders_tmp_cached asselect o_custkeyfrom o_ordersgroup by o_custkey; set hive.strict.checks.cartesian.product=false; select cntrycode, count(1) as numcust, sum(c_acctbal) as totacctbalfrom (    select cntrycode, c_acctbal,avg_acctbal from q22_customer_tmp1_cached ct1 inner join (select cntrycode, c_acctbal from q22_orders_tmp_cached ot right outer join q22_customer_tmp_cached cton ct.c_custkey = ot.o_custkeywhereo_custkey is null) ct2) awhere c_acctbal > avg_acctbalgroup by cntrycodeorder by cntrycode; set hive.strict.checks.cartesian.product=true; drop view q22_customer_tmp1_cached; drop view q22_orders_tmp_cached;"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo "--------------------------------------Finished QUERY 22."$i"--------------------------------"|& tee -a $resultPath
echo ""|& tee -a $resultPath
}

if [ ! -d $1 ] && [ ! -d $2 ] && [ ! -d $3 ]; then
cd /data/results
resultPath=hive_mr_normal_query_sf"$1"_"$3"_result.txt
rm -f $resultPath
touch $resultPath
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------" |& tee -a $resultPath
echo "-------------------------------------- "$resultPath" ---------------------------------"|& tee -a $resultPath

echo "-------------------------------------- HIVE QUERY NORMALIZED ---------------------------------"|& tee -a $resultPath
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')|& tee -a $resultPath
echo ""|& tee -a $resultPath

for i in $(seq 1 $2)
do

if ["$3" = "single"]
    for i in $(seq 1 21)
        test = query"${i}"
        eval $test
    done
fi

if ["$3" = "multiple"]
    for i in $(seq 1 21)
     query${i}
    done
fi

done
else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. 
Paramter 2 for number of loops. Parameter 3 for mode which can be single or multiple"
echo "Example ./hive_mr_normal_query.sh 1 2 single"
exit 2
fi