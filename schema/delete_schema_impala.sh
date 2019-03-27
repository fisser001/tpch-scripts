#!/bin/bash

if [ ! -d $1 ]; then
if [ "$1" = "normal" ] || [ "$1" = "all" ]; then
echo "-------------------------------------- Start deleting tables for schema normal ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table c_customer; drop table l_lineitem; drop table n_nation; drop table r_region; drop table o_orders; drop table p_part; drop table s_supplier; drop table ps_partsupp;"
echo ""
echo "-------------------------------------- Finished deleting tables for schema normal ---------------------------------"
echo ""
fi


if [ "$1" = "star" ] || [ "$1" = "all" ]; then
echo "-------------------------------------- Start deleting tables for schema star ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table c_customer_star; drop table lo_lineitem_orders_star; drop table s_supplier_star; drop table p_part_star; drop table ps_partsupp_star;"
echo ""
echo "-------------------------------------- Finished deleting tables for schema star ---------------------------------"
echo ""
fi


if [ "$1" = "denormal" ] || [ "$1" = "all" ]; then
echo "-------------------------------------- Start deleting tables for schema denormalized ---------------------------------"
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "set hive.execution.engine=mr; drop table denormalized_tmp;"
echo ""
echo "-------------------------------------- Finished deleting tables for schema denormalized ---------------------------------"
echo ""
fi

else
echo "Please specify a parameter. One of the following parameters can be specified."
echo "all or normal or star or denormal"
fi