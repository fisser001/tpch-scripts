#!/bin/bash
echo 'QUERY1-STARTING'
echo "------------------------------- STARTED QUERY 1 ----------------------------------------"
for i in 1 2
do
echo "--------------------------------------RUN" $i" started ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,  avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc, count(*) as c$) as count_order  from l_lineitem  where l_shipdate <= date '1998-12-01' - interval '90' day  group by l_returnflag, l_linestatus  order by l_returnflag, l_linestatus;"&>>/opt/data/sf1/hive_normal_query_result.txt
echo "--------------------------------------RUN" $i" finished --------------------------------"
done
echo "-------------------------------------FINISHED QUERY 1 ---------------------------------"