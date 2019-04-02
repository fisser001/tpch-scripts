--> Query 1 raw / presto / drill --> ok
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
l_lineitem
where
l_shipdate <= date '1998-12-01' - interval '90' day
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;


-- Query 1 Hive normal --> ok
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, 
sum(l_extendedprice*(1-l_discount)) as sum_disc_price, 
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, 
avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, 
avg(l_discount) as avg_disc, count(*) as count_order 
from l_lineitem 
where l_shipdate <= date '1998-12-01' - interval '90' day 
group by l_returnflag, l_linestatus 
order by l_returnflag, l_linestatus;

-- Query 1 impala normal --> ok
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, 
sum(l_extendedprice*(1-l_discount)) as sum_disc_price, 
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, 
avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, 
avg(l_discount) as avg_disc, count(*) as count_order 
from l_lineitem 
where to_date(l_shipdate) <= to_date(date_sub(cast('1998-12-01' as timestamp), interval 90 day))
group by l_returnflag, l_linestatus 
order by l_returnflag, l_linestatus;

-- Query 1 denormal hive ok
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, 
sum(l_extendedprice) as sum_base_price, 
sum(l_extendedprice*(1-l_discount)) as sum_disc_price, 
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, 
avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, 
avg(l_discount) as avg_disc, count(*) as count_order 
from denormalized
where l_shipdate <= date '1998-12-01' - interval '90' day 
group by l_returnflag, l_linestatus 
order by l_returnflag, l_linestatus;

-- Query 1 denormal drill ok
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, 
sum(l_extendedprice) as sum_base_price, 
sum(l_extendedprice*(1-l_discount)) as sum_disc_price, 
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, 
avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, 
avg(l_discount) as avg_disc, count(*) as count_order 
from denormalized 
where l_shipdate <> '""' and l_shipdate <= date '1998-12-01' - interval '90' day 
group by l_returnflag, l_linestatus 
order by l_returnflag, l_linestatus;


--> Query 1 denormal / presto /hive --> ok
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
denormalized
where
l_shipdate <= date '1998-12-01' - interval '90' day
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;

--> Query 1 denormal / impala --> ok
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
denormalized
where
to_date(l_shipdate) <= to_date(date_sub(cast('1998-12-01' as timestamp), interval 90 day))
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;

--> Query 1 star impala ok
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
lo_lineitem_orders_star
where
to_date(l_shipdate) <= to_date(date_sub(cast('1998-12-01' as timestamp), interval 90 day))
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;

--> Query 1 star  ok
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
lo_lineitem_orders_star
where
l_shipdate <= date '1998-12-01' - interval '90' day
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;

--Query 2 raw / Presto / DRILL /impala --> ok (angepasst limit 100)
select
s_acctbal,
s_name,
n_name,
p_partkey,
p_mfgr,
s_address,
s_phone,
s_comment
from
p_part,
s_supplier,
ps_partsupp,
n_nation,
r_region
where
p_partkey = ps_partkey
and s_suppkey = ps_suppkey
and p_size = 15
and p_type like '%BRASS'
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'EUROPE'
and ps_supplycost = (
select
min(ps_supplycost)
from
ps_partsupp, s_supplier,
n_nation, r_region
where
p_partkey = ps_partkey
and s_suppkey = ps_suppkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'EUROPE'
)
order by
s_acctbal desc,
n_name,
s_name,
p_partkey
limit 100;

--Query 2 normal (angepasst) --> ok
select s_acctbal,s_name, n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment
from p_part inner join ps_partsupp
on p_partkey = ps_partkey and p_size = 15 and p_type like '%BRASS'
inner join s_supplier
on s_suppkey = ps_suppkey
inner join n_nation
on s_nationkey = n_nationkey
inner join r_region
on n_regionkey = r_regionkey and r_name = 'EUROPE'
where
ps_supplycost = (select min(ps_supplycost)
from ps_partsupp, s_supplier, n_nation, r_region
where
p_partkey = ps_partkey and 
s_suppkey = ps_suppkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'EUROPE') 
order by s_acctbal desc,n_name,s_name,p_partkey limit 100;

--Query 2 denormal hive 

--Query 2 star 


--Query 3 raw / presto / drill ok angepasst limit 10
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate,
o_shippriority
from
c_customer,
o_orders,
l_lineitem
where
c_mktsegment = 'BUILDING'
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '1995-03-15'
and l_shipdate > date '1995-03-15'
group by
l_orderkey,
o_orderdate,
o_shippriority
order by
revenue desc,
o_orderdate
limit 10;

--Query 3 normal impala ok
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
to_date(o_orderdate),
o_shippriority
from
c_customer,
o_orders,
l_lineitem
where
c_mktsegment = 'BUILDING'
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and to_date(o_orderdate) < to_date('1995-03-15')
and to_date(l_shipdate) > to_date('1995-03-15')
group by
l_orderkey,
to_date(o_orderdate),
o_shippriority
order by
revenue desc,
to_date(o_orderdate)
limit 10;

-- Query 3 normal --> ok
--set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join=false;
--set hive.auto.convert.join.noconditionaltask.size=2000000000;
--set hive.auto.convert.join.noconditionaltask.size=3300000000;
--set hive.mapjoin.localtask.max.memory.usage = 0.999;
--SET hive.fetch.task.conversion=none;
select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority 
from c_customer, o_orders, l_lineitem 
where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey 
and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' 
group by l_orderkey, o_orderdate, o_shippriority 
order by revenue desc, o_orderdate limit 10;
--SET hive.fetch.task.conversion=minimal;
set hive.auto.convert.join=true;
--set hive.auto.convert.join.noconditionaltask=false;

 -- Query 3 denormal ok
select o_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority 
from denormalized
where c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' 
and l_shipdate > date '1995-03-15' 
group by o_orderkey, o_orderdate, o_shippriority 
order by revenue desc, o_orderdate limit 10;

 -- Query 3 denormal drill ok
select o_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority 
from denormalized
where c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' 
and l_shipdate <> '""' and cast(l_shipdate as DATE) > date '1995-03-15' 
group by o_orderkey, o_orderdate, o_shippriority 
order by revenue desc, o_orderdate limit 10;

 -- Query 3 denormal impala ok
select o_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, to_date(o_orderdate), o_shippriority
from denormalized
where c_mktsegment = 'BUILDING' and o_orderdate < to_date('1995-03-15')
and to_date(l_shipdate) > to_date('1995-03-15')
group by o_orderkey, to_date(o_orderdate), o_shippriority 
order by revenue desc, to_date(o_orderdate) limit 10;

--Query 3 star ok 
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate,
o_shippriority
from
c_customer_star inner join 
lo_lineitem_orders_star
on c_custkey = o_custkey
where
c_mktsegment = 'BUILDING'
and o_orderdate < date '1995-03-15'
and l_shipdate > date '1995-03-15'
group by
l_orderkey,
o_orderdate,
o_shippriority
order by
revenue desc,
o_orderdate
limit 10;

--Query 3 star impala ok 
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
to_date(o_orderdate),
o_shippriority
from
c_customer_star inner join 
lo_lineitem_orders_star
on c_custkey = o_custkey
where
c_mktsegment = 'BUILDING'
and to_date(o_orderdate) < to_date('1995-03-15')
and to_date(l_shipdate) > to_date('1995-03-15')
group by
l_orderkey,
to_date(o_orderdate),
o_shippriority
order by
revenue desc,
to_date(o_orderdate)
limit 10;

--Query 4 raw / presto / drill ok
select
o_orderpriority,
count(*) as order_count
from
o_orders
where
o_orderdate >= date '1993-07-01'
and o_orderdate < date '1993-07-01' + interval '3' month
and exists (
select
*
from
l_lineitem
where
l_orderkey = o_orderkey
and l_commitdate < l_receiptdate
)
group by
o_orderpriority
order by
o_orderpriority;

--Query 4 normal impala ok
select
o_orderpriority,
count(*) as order_count
from
o_orders
where
to_date(o_orderdate) >= to_date('1993-07-01')
and to_date(o_orderdate) < to_date(date_add(cast('1993-07-01' as timestamp), interval 3 month))
and exists (
select
*
from
l_lineitem
where
l_orderkey = o_orderkey
and to_date(l_commitdate) < to_date(l_receiptdate)
)
group by
o_orderpriority
order by
o_orderpriority;

--Query 4 normal hive --> ok
select o_orderpriority, count(*) as order_count
from o_orders
where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month 
and exists (select * from l_lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
group by o_orderpriority 
order by o_orderpriority;

-- Query 4 denormal /drill / hive ok
select o_orderpriority, count (distinct o_orderkey) as order_count 
from denormalized
where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month 
and l_commitdate < l_receiptdate 
group by o_orderpriority 
order by o_orderpriority;

-- Query 4 denormal impala ok
select o_orderpriority, count (distinct o_orderkey) as order_count 
from denormalized
where o_orderdate >= to_date('1993-07-01') and to_date(o_orderdate) < to_date(date_add(cast('1993-07-01' as timestamp), interval 3 month))
and to_date(l_commitdate) < to_date(l_receiptdate )
group by o_orderpriority 
order by o_orderpriority;

--Query 4 star ok 
select o_orderpriority, count (distinct o_orderkey) as order_count 
from lo_lineitem_orders_star
where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month 
and l_commitdate < l_receiptdate 
group by o_orderpriority 
order by o_orderpriority;

--Query 4 star impala ok 
select o_orderpriority, count (distinct o_orderkey) as order_count 
from lo_lineitem_orders_star
where o_orderdate >= to_date('1993-07-01') and to_date(o_orderdate) < to_date(date_add(cast('1993-07-01' as timestamp), interval 3 month))
and to_date(l_commitdate) < to_date(l_receiptdate )
group by o_orderpriority 
order by o_orderpriority;


--Query 5 raw / presto / drill --> ok
select
n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
c_customer,
o_orders,
l_lineitem,
s_supplier,
n_nation,
r_region
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and l_suppkey = s_suppkey
and c_nationkey = s_nationkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'ASIA'
and o_orderdate >= date '1994-01-01'
and o_orderdate < date '1994-01-01' + interval '1' year
group by
n_name
order by
revenue desc;

--Query 5 normal exp.
select
n2.n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
c_customer 
left outer join 
n_nation n1
on c_nationkey = n_nationkey
left outer join 
r_region r1
on n_regionkey = r_regionkey
left outer join
o_orders
on c_custkey = o_custkey
left outer join
l_lineitem
on l_orderkey = o_orderkey 
left outer join
s_supplier
on (l_suppkey = s_suppkey and c_nationkey = s_nationkey)
left outer join 
n_nation n2
on s_nationkey = n2.n_nationkey
left outer join 
r_region r2
on n2.n_regionkey = r2.r_regionkey
--left outer join 
--p_part
--on l_partkey = p_partkey
--left outer join 
--ps_partsupp
--on (ps_partkey = p_partkey and ps_suppkey = s_suppkey)
where
r1.r_name = 'ASIA' 
and 
r2.r_name='ASIA'
and o_orderdate >= date '1994-01-01'
and o_orderdate < date '1994-01-01' + interval '1' year
group by
n2.n_name
order by
revenue desc;

--Query 5 normal impala --> ok
select
n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
c_customer,
o_orders,
l_lineitem,
s_supplier,
n_nation,
r_region
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and l_suppkey = s_suppkey
and c_nationkey = s_nationkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'ASIA'
and to_date(o_orderdate) >= to_date('1994-01-01')
and to_date(o_orderdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
group by
n_name
order by
revenue desc;

--Query 5 normal --> ok
set hive.auto.convert.join=false;
select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue
from c_customer, o_orders, l_lineitem, s_supplier, n_nation, r_region
where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey 
and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey 
and r_name = 'ASIA' and o_orderdate >= date '1994-01-01'
and o_orderdate < date '1994-01-01' + interval '1' year
group by n_name
order by revenue desc;
set hive.auto.convert.join=true;

--> Query 5 denorm ok
select
n2_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
denormalized
where
r2_name = 'ASIA' and n_name in 
(
select
n_name
from
denormalized
where
r_name = 'ASIA' 
and o_orderdate >= date '1994-01-01'
and o_orderdate < date '1994-01-01' + interval '1' year
)
and o_orderdate >= date '1994-01-01'
and o_orderdate < date '1994-01-01' + interval '1' year
group by
n2_name
order by
revenue desc;

--> Query 5 denorm hive mr test
select
n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
denormalized
where
(r_name = 'ASIA' and r2_name = 'ASIA') --and c_custkey is not null --and o_orderdate <> '""'
and o_orderdate >= date '1994-01-01'
and o_orderdate < date '1994-01-01' + interval '1' year
group by
n_name
order by
revenue desc;

--> Query 5 denorm drill ok


--> Query 5 denorm impala ok
select
n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
denormalized
where
r_name = 'ASIA'
and to_date(o_orderdate) >= to_date('1994-01-01')
and to_date(o_orderdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
group by
n_name
order by
revenue desc;

--> Query 5 star ok
select
c.n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
c_customer_star a ,
lo_lineitem_orders_star b,
s_supplier_star c
where
a.c_custkey = b.o_custkey
and b.l_suppkey = c.s_suppkey
and c_nationkey = s_nationkey
and c.r_name = 'ASIA'
and b.o_orderdate >= date '1994-01-01'
and b.o_orderdate < date '1994-01-01' + interval '1' year
group by
c.n_name
order by
revenue desc;

--> Query 5 star impala ok
select
c.n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue
from
c_customer_star a ,
lo_lineitem_orders_star b,
s_supplier_star c
where
a.c_custkey = b.o_custkey
and b.l_suppkey = c.s_suppkey
and c_nationkey = s_nationkey
and c.r_name = 'ASIA'
and to_date(b.o_orderdate) >= to_date('1994-01-01')
and to_date(b.o_orderdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
group by
c.n_name
order by
revenue desc;

-->Query 6 raw / Presto / drill ok 
select
sum(l_extendedprice*l_discount) as revenue
from
l_lineitem
where
l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

-->Query 6 impala ok 
select
sum(l_extendedprice*l_discount) as revenue
from
l_lineitem
where
to_date(l_shipdate) >= to_date('1994-01-01')
and to_date(l_shipdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

--Query 6 normal --> ok
select sum(l_extendedprice * l_discount) as revenue
from l_lineitem
where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year 
and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;

--Query 6 denormal hive ok
select
sum(l_extendedprice*l_discount) as revenue
from
denormalized
where
l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

--Query 6 denormal drill ok
select
sum(l_extendedprice*l_discount) as revenue
from
denormalized
where l_shipdate <> '""' and
l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

--Query 6 denormal impala ok
select
sum(l_extendedprice*l_discount) as revenue
from
denormalized
where
to_date(l_shipdate) >= to_date('1994-01-01')
and to_date(l_shipdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

--Query 6 star ok
select
sum(l_extendedprice*l_discount) as revenue
from
lo_lineitem_orders_star
where
l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

--Query 6 star impala ok
select
sum(l_extendedprice*l_discount) as revenue
from
lo_lineitem_orders_star
where
to_date(l_shipdate) >= to_date('1994-01-01')
and to_date(l_shipdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
and l_discount between 0.06 - 0.01 and 0.06 + 0.01
and l_quantity < 24;

--Query 7 raw / presto / drill ok 
select
supp_nation,
cust_nation,
l_year, sum(volume) as revenue
from (
select
n1.n_name as supp_nation,
n2.n_name as cust_nation,
extract(year from l_shipdate) as l_year,
l_extendedprice * (1 - l_discount) as volume
from
s_supplier,
l_lineitem,
o_orders,
c_customer,
n_nation n1,
n_nation n2
where
s_suppkey = l_suppkey
and o_orderkey = l_orderkey
and c_custkey = o_custkey
and s_nationkey = n1.n_nationkey
and c_nationkey = n2.n_nationkey
and (
(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
)
and l_shipdate between date '1995-01-01' and date '1996-12-31'
) as shipping
group by
supp_nation,
cust_nation,
l_year
order by
supp_nation,
cust_nation,
l_year;

--Query 7 normal impala ok 
select
supp_nation,
cust_nation,
l_year, sum(volume) as revenue
from (
select
n1.n_name as supp_nation,
n2.n_name as cust_nation,
year(to_date(l_shipdate)) as l_year,
l_extendedprice * (1 - l_discount) as volume
from
s_supplier,
l_lineitem,
o_orders,
c_customer,
n_nation n1,
n_nation n2
where
s_suppkey = l_suppkey
and o_orderkey = l_orderkey
and c_custkey = o_custkey
and s_nationkey = n1.n_nationkey
and c_nationkey = n2.n_nationkey
and (
(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
)
and to_date(l_shipdate) between to_date('1995-01-01') and to_date('1996-12-31')
) as shipping
group by
supp_nation,
cust_nation,
l_year
order by
supp_nation,
cust_nation,
l_year;

--Query 7 normal --> ok --> angepasst
set hive.auto.convert.join=false;
select supp_nation, cust_nation,l_year, sum(volume) as revenue 
from 
(select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, 
(l_extendedprice * (1 - l_discount)) as volume 
from 
s_supplier inner join l_lineitem
on s_suppkey = l_suppkey
inner join o_orders
on o_orderkey = l_orderkey
inner join c_customer
on c_custkey = o_custkey
inner join n_nation n1
on s_nationkey = n1.n_nationkey
inner join n_nation n2 
on c_nationkey = n2.n_nationkey
where ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') 
or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) 
and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping 
group by supp_nation,cust_nation,l_year 
order by supp_nation, cust_nation, l_year;
set hive.auto.convert.join=true;

--Query 7 denorm hive ok
select  n2_name, n_name, 
 year(l_shipdate) as l_year, sum(l_extendedprice * (1 - l_discount)) as revenue
from denormalized
where ((n2_name = 'FRANCE' and n_name = 'GERMANY') or (n2_name = 'GERMANY' 
and n_name = 'FRANCE')) 
and l_shipdate between date '1995-01-01' and date '1996-12-31' 
group by n2_name, n_name, year(l_shipdate) 
order by n2_name, n_name, l_year;

--Query 7 denorm drill ok
select  n2_name, n_name, 
 year(l_shipdate) as l_year, sum(l_extendedprice * (1 - l_discount)) as revenue
from denormalized
where ((n2_name = 'FRANCE' and n_name = 'GERMANY') or (n2_name = 'GERMANY' 
and n_name = 'FRANCE')) and l_shipdate <> '""'
and l_shipdate between date '1995-01-01' and date '1996-12-31' 
group by n2_name, n_name, year(l_shipdate) 
order by n2_name, n_name, l_year;

--Query 7 denorm impala  ok
select  n2_name, n_name, 
 year(to_date(l_shipdate)) as l_year, sum(l_extendedprice * (1 - l_discount)) as revenue
from denormalized
where ((n2_name = 'FRANCE' and n_name = 'GERMANY') or (n2_name = 'GERMANY' 
and n_name = 'FRANCE')) 
and to_date(l_shipdate) between to_date('1995-01-01') and to_date('1996-12-31')
group by n2_name, n_name, year(to_date(l_shipdate))
order by n2_name, n_name, l_year;

--Query 7 star impala  ok
select
supp_nation,
cust_nation,
l_year, sum(volume) as revenue
from (
select
a.n_name as supp_nation,
c.n_name as cust_nation,
year(to_date(l_shipdate)) as l_year,
b.l_extendedprice * (1 - b.l_discount) as volume
from
s_supplier_star a,
lo_lineitem_orders_star b,
c_customer_star c
where
a.s_suppkey = b.l_suppkey
and c.c_custkey = b.o_custkey
and (
(a.n_name = 'FRANCE' and c.n_name = 'GERMANY')
or (a.n_name = 'GERMANY' and c.n_name = 'FRANCE')
)
and to_date(l_shipdate) between to_date('1995-01-01') and to_date('1996-12-31')
) as shipping
group by
supp_nation,
cust_nation,
l_year
order by
supp_nation,
cust_nation,
l_year;

--Query 7 star  ok
select
supp_nation,
cust_nation,
l_year, sum(volume) as revenue
from (
select
a.n_name as supp_nation,
c.n_name as cust_nation,
extract(year from b.l_shipdate) as l_year,
b.l_extendedprice * (1 - b.l_discount) as volume
from
s_supplier_star a,
lo_lineitem_orders_star b,
c_customer_star c
where
a.s_suppkey = b.l_suppkey
and c.c_custkey = b.o_custkey
and (
(a.n_name = 'FRANCE' and c.n_name = 'GERMANY')
or (a.n_name = 'GERMANY' and c.n_name = 'FRANCE')
)
and b.l_shipdate between date '1995-01-01' and date '1996-12-31'
) as shipping
group by
supp_nation,
cust_nation,
l_year
order by
supp_nation,
cust_nation,
l_year;

--Query 8 raw / presto / drill ok
select
o_year,
sum(case
when nation = 'BRAZIL'
then volume
else 0
end) / sum(volume) as mkt_share
from (
select
extract(year from o_orderdate) as o_year,
l_extendedprice * (1-l_discount) as volume,
n2.n_name as nation
from
p_part,
s_supplier,
l_lineitem,
o_orders,
c_customer,
n_nation n1,
n_nation n2,
r_region
where
p_partkey = l_partkey
and s_suppkey = l_suppkey
and l_orderkey = o_orderkey
and o_custkey = c_custkey
and c_nationkey = n1.n_nationkey
and n1.n_regionkey = r_regionkey
and r_name = 'AMERICA'
and s_nationkey = n2.n_nationkey
and o_orderdate between date '1995-01-01' and date '1996-12-31'
and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
o_year
order by
o_year;

--Query 8 impala ok
select
o_year,
sum(case
when nation = 'BRAZIL'
then volume
else 0
end) / sum(volume) as mkt_share
from (
select
year(to_date(o_orderdate)) as o_year,
l_extendedprice * (1-l_discount) as volume,
n2.n_name as nation
from
p_part,
s_supplier,
l_lineitem,
o_orders,
c_customer,
n_nation n1,
n_nation n2,
r_region
where
p_partkey = l_partkey
and s_suppkey = l_suppkey
and l_orderkey = o_orderkey
and o_custkey = c_custkey
and c_nationkey = n1.n_nationkey
and n1.n_regionkey = r_regionkey
and r_name = 'AMERICA'
and s_nationkey = n2.n_nationkey
and to_date(o_orderdate) between to_date('1995-01-01') and to_date('1996-12-31')
and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
o_year
order by
o_year;

--Query 8 normal --> ok --> angepasst
select o_year, sum(case	when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from 
(select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, 
n2.n_name as nation
from p_part inner join l_lineitem
on p_partkey = l_partkey
inner join s_supplier
on s_suppkey = l_suppkey
inner join o_orders
on l_orderkey = o_orderkey
inner join c_customer
on o_custkey = c_custkey
inner join n_nation n1
on c_nationkey = n1.n_nationkey
inner join n_nation n2
on s_nationkey = n2.n_nationkey
inner join r_region
on n1.n_regionkey = r_regionkey
where  r_name = 'AMERICA' and o_orderdate between date '1995-01-01' and date '1996-12-31' 
and p_type = 'ECONOMY ANODIZED STEEL') as all_nations
group by o_year
order by o_year;

--Query 8 denorm drill hive ok
select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share 
from ( select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, 
n2_name as nation 
from denormalized where r_name = 'AMERICA' and o_orderdate between date '1995-01-01' 
and date '1996-12-31' 
and p_type = 'ECONOMY ANODIZED STEEL') as all_nations 
group by o_year 
order by  o_year;

--Query 8 denorm impala ok
select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share 
from ( select year(to_date(o_orderdate)) as o_year, l_extendedprice * (1-l_discount) as volume, 
n2_name as nation 
from denormalized where r_name = 'AMERICA' and to_date(o_orderdate) between to_date('1995-01-01')
and to_date('1996-12-31')
and p_type = 'ECONOMY ANODIZED STEEL') as all_nations 
group by o_year 
order by  o_year;

--Query 8 star ok 
--for Hive
set hive.strict.checks.cartesian.product=false;
select
o_year,
sum(case
when nation = 'BRAZIL'
then volume
else 0
end) / sum(volume) as mkt_share
from (
select
extract(year from o_orderdate) as o_year,
l_extendedprice * (1-l_discount) as volume,
b.n_name as nation
from
p_part_star a,
s_supplier_star b,
lo_lineitem_orders_star c,
c_customer_star d
where
a.p_partkey = c.l_partkey
and b.s_suppkey = c.l_suppkey
and c.o_custkey = d.c_custkey
and d.r_name = 'AMERICA'
and c.o_orderdate between date '1995-01-01' and date '1996-12-31'
and a.p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
o_year
order by
o_year;

--Query 8 star impala ok
select
o_year,
sum(case
when nation = 'BRAZIL'
then volume
else 0
end) / sum(volume) as mkt_share
from (
select
year(to_date(o_orderdate)) as o_year,
l_extendedprice * (1-l_discount) as volume,
b.n_name as nation
from
p_part_star a,
s_supplier_star b,
lo_lineitem_orders_star c,
c_customer_star d
where
a.p_partkey = c.l_partkey
and b.s_suppkey = c.l_suppkey
and c.o_custkey = d.c_custkey
and d.r_name = 'AMERICA'
and c.o_orderdate between to_date('1995-01-01') and to_date('1996-12-31')
and a.p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
o_year
order by
o_year;

--Query 9 raw / presto / drill ok
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
n_name as nation,
extract(year from o_orderdate) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from
p_part,
s_supplier,
l_lineitem,
ps_partsupp,
o_orders,
n_nation
where
s_suppkey = l_suppkey
and ps_suppkey = l_suppkey
and ps_partkey = l_partkey
and p_partkey = l_partkey
and o_orderkey = l_orderkey
and s_nationkey = n_nationkey
and p_name like '%green%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;

--Query 9 normal impala ok
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
n_name as nation,
year(to_date(o_orderdate)) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from
p_part,
s_supplier,
l_lineitem,
ps_partsupp,
o_orders,
n_nation
where
s_suppkey = l_suppkey
and ps_suppkey = l_suppkey
and ps_partkey = l_partkey
and p_partkey = l_partkey
and o_orderkey = l_orderkey
and s_nationkey = n_nationkey
and p_name like '%green%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;

--Query 9 normal --> ok angepasst
set hive.auto.convert.join=false;
select nation, o_year, sum(amount) as sum_profit
from 
(select n_name as nation, extract(year from o_orderdate) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from p_part inner join l_lineitem
on p_partkey = l_partkey
inner join s_supplier
on s_suppkey = l_suppkey
inner join ps_partsupp
on ps_suppkey = l_suppkey and ps_partkey = l_partkey 
inner join o_orders
on o_orderkey = l_orderkey
inner join n_nation
on s_nationkey = n_nationkey
where p_name like '%green%') as profit
group by nation, o_year 
order by nation, o_year desc;
set hive.auto.convert.join=true;

--Query 9 denormal ok
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
n2_name as nation,
extract(year from o_orderdate) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from
denormalized
where
p_name like '%green%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;

--Query 9 denormal impala ok
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
n2_name as nation,
year(to_date(o_orderdate)) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from
denormalized
where
p_name like '%green%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;

--Query 9 star ok
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
b.n_name as nation,
extract(year from c.o_orderdate) as o_year,
c.l_extendedprice * (1 - c.l_discount) - d.ps_supplycost * c.l_quantity as amount
from
p_part_star a,
s_supplier_star b,
lo_lineitem_orders_star c,
ps_partsupp_star d
where
b.s_suppkey = c.l_suppkey
and d.ps_suppkey = c.l_suppkey
and d.ps_partkey = c.l_partkey
and a.p_partkey = c.l_partkey
and a.p_name like '%green%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;

--Query 9 star impala ok
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
b.n_name as nation,
year(to_date(c.o_orderdate)) as o_year,
c.l_extendedprice * (1 - c.l_discount) - d.ps_supplycost * c.l_quantity as amount
from
p_part_star a,
s_supplier_star b,
lo_lineitem_orders_star c,
ps_partsupp_star d
where
b.s_suppkey = c.l_suppkey
and d.ps_suppkey = c.l_suppkey
and d.ps_partkey = c.l_partkey
and a.p_partkey = c.l_partkey
and a.p_name like '%green%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;

--Query 10 raw / presto / drill ok
select
c_custkey,
c_name,
sum(l_extendedprice * (1 - l_discount)) as revenue,
c_acctbal,
n_name,
c_address,
c_phone,
c_comment
from
c_customer,
o_orders,
l_lineitem,
n_nation
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate >= date '1993-10-01'
and o_orderdate < date '1993-10-01' + interval '3' month
and l_returnflag = 'R'
and c_nationkey = n_nationkey
group by
c_custkey,
c_name,
c_acctbal,
c_phone,
n_name,
c_address,
c_comment
order by
revenue desc limit 20;

--Query 10 impala ok
select
c_custkey,
c_name,
sum(l_extendedprice * (1 - l_discount)) as revenue,
c_acctbal,
n_name,
c_address,
c_phone,
c_comment
from
c_customer,
o_orders,
l_lineitem,
n_nation
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and to_date(o_orderdate) >= to_date('1993-10-01')
and to_date(o_orderdate) < to_date(date_add(cast('1993-10-01' as timestamp), interval 3 month))
and l_returnflag = 'R'
and c_nationkey = n_nationkey
group by
c_custkey,
c_name,
c_acctbal,
c_phone,
n_name,
c_address,
c_comment
order by
revenue desc limit 20;

--Query 10 --> ok --> angepasst
set hive.auto.convert.join=false;
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment
from c_customer, o_orders, l_lineitem, n_nation
where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' 
and o_orderdate < date '1993-10-01' + interval '3' month 
and l_returnflag = 'R' and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal,c_phone,n_name,c_address, c_comment
order by revenue desc limit 20;
set hive.auto.convert.join=true;


--Query 10 denorm hive ok
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, 
n_name, c_address, c_phone, c_comment
from denormalized 
where o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month 
and l_returnflag = 'R' 
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment 
order by revenue 
desc limit 20;

--Query 10 denorm impala ok
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, 
n_name, c_address, c_phone, c_comment
from denormalized 
where 
to_date(o_orderdate) >= to_date(cast('1993-10-01' as timestamp)) and
to_date(o_orderdate) < to_date(date_add(cast('1993-10-01' as timestamp), interval 3 month))
and l_returnflag = 'R' 
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment 
order by revenue 
desc limit 20;

--Query 10 star ok
select
a.c_custkey,
a.c_name,
sum(b.l_extendedprice * (1 - b.l_discount)) as revenue,
a.c_acctbal,
a.n_name,
a.c_address,
a.c_phone,
a.c_comment
from
c_customer_star a,
lo_lineitem_orders_star b
where
a.c_custkey = b.o_custkey
and b.o_orderdate >= date '1993-10-01'
and b.o_orderdate < date '1993-10-01' + interval '3' month
and b.l_returnflag = 'R'
group by
a.c_custkey,
a.c_name,
a.c_acctbal,
a.c_phone,
a.n_name,
a.c_address,
a.c_comment
order by
revenue desc limit 20;

--Query 10 star impala ok
select
a.c_custkey,
a.c_name,
sum(b.l_extendedprice * (1 - b.l_discount)) as revenue,
a.c_acctbal,
a.n_name,
a.c_address,
a.c_phone,
a.c_comment
from
c_customer_star a,
lo_lineitem_orders_star b
where
a.c_custkey = b.o_custkey
and b.o_orderdate >= to_date('1993-10-01')
and b.o_orderdate < to_date(date_add(cast('1993-10-01' as timestamp), interval 3 month))
and b.l_returnflag = 'R'
group by
a.c_custkey,
a.c_name,
a.c_acctbal,
a.c_phone,
a.n_name,
a.c_address,
a.c_comment
order by
revenue desc limit 20;

--Query 11 raw / presto / drill ok --> angepasst für presto value --> value1
select
ps_partkey,
sum(ps_supplycost * ps_availqty) as value1
from
ps_partsupp,
s_supplier,
n_nation
where
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
group by
ps_partkey having
sum(ps_supplycost * ps_availqty) > (
select
sum(ps_supplycost * ps_availqty) * 0.0001
from
ps_partsupp,
s_supplier,
n_nation
where
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
)
order by
value1 desc;

--Query 11 normal --> ok
create view q11_part_tmp_cached as
select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value
from ps_partsupp,s_supplier,n_nation
where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
group by ps_partkey;

create view q11_sum_tmp_cached as
select sum(part_value) as total_value
from q11_part_tmp_cached;

set hive.strict.checks.cartesian.product=false;
select ps_partkey, part_value as value
from (select ps_partkey,part_value,total_value 
from q11_part_tmp_cached join q11_sum_tmp_cached
) a
where part_value > total_value * 0.0001
order by
value desc;
set hive.strict.checks.cartesian.product=true;

--Query 11 normal impala --> ok
create view q11_part_tmp_cached as
select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value
from ps_partsupp,s_supplier,n_nation
where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
group by ps_partkey;

create view q11_sum_tmp_cached as
select sum(part_value) as total_value
from q11_part_tmp_cached;

select ps_partkey, part_value as value
from (select ps_partkey,part_value,total_value 
from q11_part_tmp_cached join q11_sum_tmp_cached
) a
where part_value > total_value * 0.0001
order by
value desc;

--Query 11 denorm

--Query 11 star ok
select
ps_partkey,
sum(ps_supplycost * ps_availqty) as value
from
ps_partsupp_star,
s_supplier_star
where
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
group by
ps_partkey having
sum(ps_supplycost * ps_availqty) > (
select
sum(ps_supplycost * ps_availqty) * 0.0001
from
ps_partsupp_star,
s_supplier_star
where
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
)
order by
value desc;

--Query 11 star impala ok
create view q11_part_tmp_cached as
select ps_partkey,sum(ps_supplycost * ps_availqty) as part_value
from ps_partsupp_star,s_supplier_star
where ps_suppkey = s_suppkey and n_name = 'GERMANY'
group by ps_partkey;

create view q11_sum_tmp_cached as
select sum(part_value) as total_value
from q11_part_tmp_cached;

select ps_partkey, part_value as value
from (select ps_partkey,part_value,total_value 
from q11_part_tmp_cached join q11_sum_tmp_cached
) a
where part_value > total_value * 0.0001
order by
value desc;

drop view q11_part_tmp_cached;
drop view q11_sum_tmp_cached;

--Query 12 raw / presto / drill ok
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
o_orders,
l_lineitem
where
o_orderkey = l_orderkey
and l_shipmode in ('MAIL', 'SHIP')
and l_commitdate < l_receiptdate
and l_shipdate < l_commitdate
and l_receiptdate >= date '1994-01-01'
and l_receiptdate < date '1994-01-01' + interval '1' year
group by
l_shipmode
order by
l_shipmode;

--Query 12 normal impala ok
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
o_orders,
l_lineitem
where
o_orderkey = l_orderkey
and l_shipmode in ('MAIL', 'SHIP')
and to_date(l_commitdate) < to_date(l_receiptdate)
and to_date(l_shipdate) < to_date(l_commitdate)
and to_date(l_receiptdate) >= to_date('1994-01-01')
and to_date(l_receiptdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
group by
l_shipmode
order by
l_shipmode;

--Query 12 normal --> ok
set hive.auto.convert.join=false;
select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from o_orders, l_lineitem
where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate
and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode;
set hive.auto.convert.join=true;

--Query 12 denormal hive ok
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
denormalized
where l_shipdate <> '""' and
l_shipmode in ('MAIL', 'SHIP')
and l_commitdate < l_receiptdate
and l_shipdate < l_commitdate
and l_receiptdate >= date '1994-01-01'
and l_receiptdate < date '1994-01-01' + interval '1' year
group by
l_shipmode
order by
l_shipmode;


--Query 12 denormal hive ok
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
denormalized
where l_shipmode in ('MAIL', 'SHIP')
and l_commitdate < l_receiptdate
and l_shipdate < l_commitdate
and l_receiptdate >= date '1994-01-01'
and l_receiptdate < date '1994-01-01' + interval '1' year
group by
l_shipmode
order by
l_shipmode;

--Query 12 denormal impala ok
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
denormalized
where
l_shipmode in ('MAIL', 'SHIP')
and to_date(l_commitdate) < to_date(l_receiptdate)
and to_date(l_shipdate) < to_date(l_commitdate)
and to_date(l_receiptdate) >= to_date('1994-01-01')
and to_date(l_receiptdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
group by
l_shipmode
order by
l_shipmode;

--Query 12 star ok 
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
lo_lineitem_orders_star
where
l_shipmode in ('MAIL', 'SHIP')
and l_commitdate < l_receiptdate
and l_shipdate < l_commitdate
and l_receiptdate >= date '1994-01-01'
and l_receiptdate < date '1994-01-01' + interval '1' year
group by
l_shipmode
order by
l_shipmode;

--Query 12 star impala ok 
select
l_shipmode,
sum(case
when o_orderpriority ='1-URGENT'
or o_orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when o_orderpriority <> '1-URGENT'
and o_orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
lo_lineitem_orders_star
where
l_shipmode in ('MAIL', 'SHIP')
and to_date(l_commitdate) < to_date(l_receiptdate)
and l_shipdate < l_commitdate
and to_date(l_receiptdate) >= to_date('1994-01-01')
and to_date(l_receiptdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
group by
l_shipmode
order by
l_shipmode;

--Query 13 raw / presto / drill ok
select
c_count, count(*) as custdist
from (
select
c_custkey,
count(o_orderkey)
from
c_customer left outer join o_orders on
c_custkey = o_custkey
and o_comment not like '%special%requests%'
group by
c_custkey
)as c_orders (c_custkey, c_count)
group by
c_count
order by
custdist desc,
c_count desc;

--Query 13 normal hive/impala --> ok --> angepasst
select c_count, count(*) as custdist 
from (
select c_custkey,count(o_orderkey) as c_count
from c_customer 
left outer join o_orders 
on c_custkey = o_custkey
and o_comment not like '%special%requests%'
group by c_custkey) as c_orders
group by c_count
order by custdist desc, c_count desc;

--Query 13 denormal 

--Query 13 star hive / impala
select
c_count, count(*) as custdist
from (
select
c_custkey,
count(distinct o_orderkey) as c_count
from
c_customer_star left outer join lo_lineitem_orders_star on
c_custkey = o_custkey
and o_orderkey is not null
and o_comment not like '%special%requests%'
group by
c_custkey
)as c_orders
group by
c_count
order by
custdist desc,
c_count desc;

--Query 13 star ok presto
select
c_count, count(*) as custdist
from (
select
c_custkey,
count(distinct o_orderkey)
from
c_customer_star left outer join lo_lineitem_orders_star on
c_custkey = o_custkey
and o_orderkey is not null
and o_comment not like '%special%requests%'
group by
c_custkey
)as c_orders (c_custkey, c_count)
group by
c_count
order by
custdist desc,
c_count desc;


--Query 14 raw presto / drill
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
l_lineitem,
p_part
where
l_partkey = p_partkey
and l_shipdate >= date '1995-09-01'
and l_shipdate < date '1995-09-01' + interval '1' month;

--Query 14 normal impala ok
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
l_lineitem,
p_part
where
l_partkey = p_partkey
and to_date(l_shipdate) >= to_date('1995-09-01')
and to_date(l_shipdate) < to_date(date_add(cast('1995-09-01' as timestamp), interval 1 month));

--Query 14 normal --> ok
select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from l_lineitem, p_part 
where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' 
and l_shipdate < date '1995-09-01' + interval '1' month;

--Query 14 denormal ok
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
denormalized
where
l_partkey = p_partkey
and l_shipdate >= date '1995-09-01'
and l_shipdate < date '1995-09-01' + interval '1' month;

--Query 14 denormal drill ok
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
denormalized
where
l_partkey = p_partkey and l_shipdate <> '""'
and l_shipdate >= date '1995-09-01'
and l_shipdate < date '1995-09-01' + interval '1' month;

--Query 14 denormal impala ok
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
denormalized
where
l_partkey = p_partkey
and to_date(l_shipdate) >= to_date('1995-09-01')
and to_date(l_shipdate) < to_date(date_add(cast('1995-09-01' as timestamp), interval 1 month));

--Query 14 star ok 
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
lo_lineitem_orders_star,
p_part_star
where
l_partkey = p_partkey
and l_shipdate >= date '1995-09-01'
and l_shipdate < date '1995-09-01' + interval '1' month;

--Query 14 star ok 
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
lo_lineitem_orders_star,
p_part_star
where
l_partkey = p_partkey
and l_shipdate >= date '1995-09-01'
and l_shipdate < date '1995-09-01' + interval '1' month;

--Query 14 star impala ok 
select
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount)
else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
lo_lineitem_orders_star,
p_part_star
where
l_partkey = p_partkey
and to_date(l_shipdate) >= to_date('1995-09-01')
and to_date(l_shipdate) < to_date(date_add(cast('1995-09-01' as timestamp), interval 1 month));

--Query 15 raw / presto / drill --> nicht ok --> views are not supported
view v_revenue (supplier_no, total_revenue) as
select
l_suppkey,
sum(l_extendedprice * (1 - l_discount))
from
lineitem
where
l_shipdate >= date '1996-01-01'
and l_shipdate < date '1996-01-01' + interval '3' month
group by
l_suppkey;

select
s_suppkey,
s_name,
s_address,
s_phone,
total_revenue
from
supplier,
v_revenue
where
s_suppkey = supplier_no
and total_revenue = (
select
max(total_revenue)
from
v_revenue
)
order by
s_suppkey;

drop view v_revenue;

--Query 15 normal impala ok
create view v_revenue (supplier_no, total_revenue) as
select
l_suppkey,
sum(l_extendedprice * (1 - l_discount))
from
l_lineitem
where
to_date(l_shipdate) >= to_date('1996-01-01')
and to_date(l_shipdate) < to_date(date_add(cast('1996-01-01' as timestamp), interval 3 month))
group by
l_suppkey;

select
s_suppkey,
s_name,
s_address,
s_phone,
total_revenue
from
s_supplier,
v_revenue
where
s_suppkey = supplier_no
and total_revenue = (
select
max(total_revenue)
from
v_revenue
)
order by
s_suppkey;

--Query 15 normal --> ok --> angepasst
create view v_revenue (supplier_no, total_revenue) as
select l_suppkey,sum(l_extendedprice * (1 - l_discount))
from denormalized	
where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
group by l_suppkey;

select s_suppkey,s_name,s_address,s_phone,max(total_revenue) as total_revenue
from denormalized, v_revenue 
where s_suppkey = supplier_no
group by s_suppkey,s_name,s_address,s_phone
order by total_revenue desc limit 1;

--Query 15 denormal hive ok ; presto not working because of view
set hive.auto.convert.join=false;
create view v_revenue_denorm (supplier_no, total_revenue) as
select l_suppkey,sum(l_extendedprice * (1 - l_discount))
from denormalized	
where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
group by l_suppkey;

select s_suppkey,s_name,s_address,s_phone,max(total_revenue) as total_revenue
from denormalized, v_revenue_denorm 
where s_suppkey = supplier_no
group by s_suppkey,s_name,s_address,s_phone
order by total_revenue desc limit 1;
set hive.auto.convert.join=true;

--Query 15 denormal impala ok  
create view v_revenue_denorm (supplier_no, total_revenue) as
select l_suppkey,sum(l_extendedprice * (1 - l_discount))
from denormalized	
where to_date(l_shipdate) >= to_date('1996-01-01') 
and to_date(l_shipdate) < to_date(date_add(cast('1996-01-01' as timestamp), interval 3 month))
group by l_suppkey;

select s_suppkey,s_name,s_address,s_phone,max(total_revenue) as total_revenue
from denormalized, v_revenue_denorm 
where s_suppkey = supplier_no
group by s_suppkey,s_name,s_address,s_phone
order by total_revenue desc limit 1;

drop view v_revenue_denorm;

--Query 15 denormal for presto ok aber angepasst
select l_suppkey as supplier_no,sum(l_extendedprice * (1 - l_discount)) as total_revenue,
s_name,s_address,s_phone
from denormalized	
where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
group by l_suppkey, s_name,s_address,s_phone
order by total_revenue desc limit 1;

--Query 15 denormal for drill + hive ok aber angepasst
select l_suppkey as supplier_no,sum(l_extendedprice * (1 - l_discount)) as total_revenue,
s_name,s_address,s_phone
from denormalized	
where l_shipdate <> '""' and l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
group by l_suppkey, s_name,s_address,s_phone
order by total_revenue desc limit 1;

--Query 15 star hive
view v_revenue_star (supplier_no, total_revenue) as
select
l_suppkey,
sum(l_extendedprice * (1 - l_discount))
from
lo_lineitem_orders_star
where
l_shipdate >= date '1996-01-01'
and l_shipdate < date '1996-01-01' + interval '3' month
group by
l_suppkey;

select
s_suppkey,
s_name,
s_address,
s_phone,
total_revenue
from
s_supplier_star,
v_revenue_star
where
s_suppkey = supplier_no
and total_revenue = (
select
max(total_revenue)
from
v_revenue_star
)
order by
s_suppkey;

drop view v_revenue;

--Query 15 star impala
create view v_revenue_star (supplier_no, total_revenue) as
select
l_suppkey,
sum(l_extendedprice * (1 - l_discount))
from
lo_lineitem_orders_star
where
to_date(l_shipdate) >= to_date('1996-01-01')
and l_shipdate < to_date(date_add(cast('1996-01-01' as timestamp), interval 3 month))
group by
l_suppkey;

select
s_suppkey,
s_name,
s_address,
s_phone,
total_revenue
from
s_supplier_star,
v_revenue_star
where
s_suppkey = supplier_no
and total_revenue = (
select
max(total_revenue)
from
v_revenue_star
)
order by
s_suppkey;

--Query 15 star presto --> not possible due to  no views

--Query 16 raw / presto / drill /impala  added limit ok
select
p_brand,
p_type,
p_size,
count(distinct ps_suppkey) as supplier_cnt
from
ps_partsupp,
p_part
where
p_partkey = ps_partkey
and p_brand <> 'Brand#45'
and p_type not like 'MEDIUM POLISHED%' 
and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
and ps_suppkey not in (
select
s_suppkey
from
s_supplier
where
s_comment like '%Customer%Complaints%'
)
group by
p_brand,
p_type,
p_size
order by
supplier_cnt desc,
p_brand,
p_type,
p_size limit 20000;

--Query 16 normal --> ok --> angepasst
select p_brand, p_type,p_size, count(distinct ps_suppkey) as supplier_cnt
from ps_partsupp inner join p_part 
on p_partkey = ps_partkey 
left outer join s_supplier
on ps_suppkey = s_suppkey
where s_comment not like '%Customer%Complaints%'
and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' 
and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
group by p_brand,p_type,p_size
order by supplier_cnt desc, p_brand,p_type, p_size limit 20000;

--Query 16 denorm impala / drill ok
select p_brand, p_type, p_size, count(distinct s_suppkey) as supplier_cnt 
from denormalized 
where p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' 
and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and s_comment not like '%Customer%Complaints%'
group by p_brand, p_type, p_size 
order by  supplier_cnt desc,  p_brand,  p_type, p_size;

--Query 16 star hive ok
set hive.strict.checks.cartesian.product=false;
select
p_brand,
p_type,
p_size,
count(distinct ps_suppkey) as supplier_cnt
from
ps_partsupp_star inner join 
p_part_star
on 
p_partkey = ps_partkey
where
p_brand <> 'Brand#45'
and p_type not like 'MEDIUM POLISHED%' 
and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
and ps_suppkey not in (
select
s_suppkey
from
s_supplier_star
where
s_comment like '%Customer%Complaints%'
)
group by
p_brand,
p_type,
p_size
order by
supplier_cnt desc,
p_brand,
p_type,
p_size limit 20000;
set hive.strict.checks.cartesian.product=true;


--Query 16 star drill / impala ok
select
p_brand,
p_type,
p_size,
count(distinct ps_suppkey) as supplier_cnt
from
ps_partsupp_star,
p_part_star
where
p_partkey = ps_partkey
and p_brand <> 'Brand#45'
and p_type not like 'MEDIUM POLISHED%' 
and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
and ps_suppkey not in (
select
s_suppkey
from
s_supplier_star
where
s_comment like '%Customer%Complaints%'
)
group by
p_brand,
p_type,
p_size
order by
supplier_cnt desc,
p_brand,
p_type,
p_size limit 20000;

--Query 17 raw / presto / drill /impala
select
sum(l_extendedprice) / 7.0 as avg_yearly
from
l_lineitem,
p_part
where
p_partkey = l_partkey and 
p_brand = 'Brand#23' and p_container = 'MED BOX' 
and l_quantity < (
select
0.2 * avg(l_quantity)
from
l_lineitem
where
l_partkey = p_partkey
);

--Query 17 normal --> ok
select sum(l_extendedprice) / 7.0 as avg_yearly
from l_lineitem, p_part
where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' 
and l_quantity < (
select 0.2 * avg(l_quantity)
from l_lineitem
where l_partkey = p_partkey);

--Query 17 denorm impala / hive/ drill ok
select
sum(l_extendedprice) / 7.0 as avg_yearly
from
denormalized a
where
p_brand = 'Brand#23' and p_container = 'MED BOX' 
and l_quantity < (
select
0.2 * avg(l_quantity)
from
denormalized b
where
a.l_partkey = b.p_partkey
);

--Query 17 star drill/impala /hive ok
select
sum(l_extendedprice) / 7.0 as avg_yearly
from
lo_lineitem_orders_star,
p_part_star
where
p_partkey = l_partkey and 
p_brand = 'Brand#23' and p_container = 'MED BOX' 
and l_quantity < (
select
0.2 * avg(l_quantity)
from
lo_lineitem_orders_star
where
l_partkey = p_partkey
);


--Query 18 raw / presto / drill impala ok
select
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from
c_customer,
o_orders,
l_lineitem
where
o_orderkey in (
select
l_orderkey
from
l_lineitem
group by
l_orderkey having
sum(l_quantity) > 300
)
and c_custkey = o_custkey
and o_orderkey = l_orderkey
group by
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice
order by
o_totalprice desc,
o_orderdate limit 100;

--Query 18 normal --> ok 
select c_name,c_custkey,o_orderkey, o_orderdate,o_totalprice,sum(l_quantity) 
from c_customer, o_orders, l_lineitem 
where o_orderkey in (
select l_orderkey 
from l_lineitem
group by l_orderkey 
having sum(l_quantity) > 300)
and c_custkey = o_custkey
and o_orderkey = l_orderkey
group by c_name,c_custkey, o_orderkey, o_orderdate,o_totalprice
order by o_totalprice desc, o_orderdate limit 100;

--Query 18 denorm drill hive ok
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) 
from denormalized
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice 
having sum(l_quantity) > 300 
order by o_totalprice desc, o_orderdate;

--Query 18 denorm impala ok
select c_name, c_custkey, o_orderkey, to_date(o_orderdate), o_totalprice, sum(l_quantity) 
from denormalized
group by c_name, c_custkey, o_orderkey, to_date(o_orderdate), o_totalprice 
having sum(l_quantity) > 300 
order by o_totalprice desc, to_date(o_orderdate);

--Query 18 star impala / hive ok
select
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from
c_customer_star,
lo_lineitem_orders_star
where
o_orderkey in (
select
l_orderkey
from
lo_lineitem_orders_star
group by
l_orderkey having
sum(l_quantity) > 300
)
and c_custkey = o_custkey
group by
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice
order by
o_totalprice desc,
o_orderdate limit 100;

--Query 19 raw / presto / impala / drill (not WORKING!!!!!!!)
select
sum(l_extendedprice * (1 - l_discount) ) as revenue
from
l_lineitem,
p_part
where
(
p_partkey = l_partkey
and p_brand = 'Brand#12'
and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
and l_quantity >= 1 and l_quantity <= 1 + 10
and p_size between 1 and 5
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_partkey = l_partkey
and p_brand = 'Brand#23'
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10 and l_quantity <= 10 + 10
and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_partkey = l_partkey
and p_brand = 'Brand#34'
and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20 and l_quantity <= 20 + 10
and p_size between 1 and 15
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
);

--Query 19 raw / drill angepasst 
select
sum(l_extendedprice * (1 - l_discount) ) as revenue
from
l_lineitem inner join 
p_part
on 
p_partkey = l_partkey
where
(
p_brand = 'Brand#12'
and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
and l_quantity >= 1 and l_quantity <= 1 + 10
and p_size between 1 and 5
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_brand = 'Brand#23'
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10 and l_quantity <= 10 + 10
and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_brand = 'Brand#34'
and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20 and l_quantity <= 20 + 10
and p_size between 1 and 15
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
);

--Query 19 normal hive --> ok
select sum(l_extendedprice* (1 - l_discount)) as revenue
from l_lineitem, p_part
where (p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
and l_quantity >= 1 and l_quantity <= 1 + 10
and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON') or (
p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (
p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 
and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON');

--Query 19 denorm impala drill ok
select sum(l_extendedprice * (1 - l_discount) ) as revenue 
from denormalized
where ( p_brand = 'Brand#12'and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') 
and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') 
and l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#23' 
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 
and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') 
and l_shipinstruct = 'DELIVER IN PERSON' ) or (p_brand = 'Brand#34' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') 
and l_shipinstruct = 'DELIVER IN PERSON');

--Query 19 star impala ok
select
sum(l_extendedprice * (1 - l_discount) ) as revenue
from
lo_lineitem_orders_star,
p_part_star
where
(
p_partkey = l_partkey
and p_brand = 'Brand#12'
and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
and l_quantity >= 1 and l_quantity <= 1 + 10
and p_size between 1 and 5
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_partkey = l_partkey
and p_brand = 'Brand#23'
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10 and l_quantity <= 10 + 10
and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_partkey = l_partkey
and p_brand = 'Brand#34'
and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20 and l_quantity <= 20 + 10
and p_size between 1 and 15
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
);

--Query 19 star drill ok
select
sum(l_extendedprice * (1 - l_discount) ) as revenue
from
lo_lineitem_orders_star inner join p_part_star
on p_partkey = l_partkey
where
(
p_brand = 'Brand#12'
and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
and l_quantity >= 1 and l_quantity <= 1 + 10
and p_size between 1 and 5
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_brand = 'Brand#23'
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10 and l_quantity <= 10 + 10
and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or
(
p_brand = 'Brand#34'
and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20 and l_quantity <= 20 + 10
and p_size between 1 and 15
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
);



--Query 20 raw / presto --> drill not working
select
s_name,
s_address
from
s_supplier, n_nation
where
s_suppkey in (
select
ps_suppkey
from
ps_partsupp
where
ps_partkey in (
select
p_partkey
from
p_part
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
l_lineitem
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= date('1994-01-01')
and l_shipdate < date('1994-01-01') + interval '1' year
)
)
and s_nationkey = n_nationkey
and n_name = 'CANADA'
order by
s_name;

--Query 20 raw / drill ok
select
s_name,
s_address
from
s_supplier, n_nation
where
s_suppkey in (
select
ps_suppkey
from
ps_partsupp
where
ps_partkey in (
select
p_partkey
from
p_part
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
l_lineitem
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
)
)
and s_nationkey = n_nationkey
and n_name = 'CANADA'
order by
s_name;

--Query 20 normal impala ok
select
s_name,
s_address
from
s_supplier, n_nation
where
s_suppkey in (
select
ps_suppkey
from
ps_partsupp
where
ps_partkey in (
select
p_partkey
from
p_part
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
l_lineitem
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and to_date(l_shipdate) >= to_date('1994-01-01')
and to_date(l_shipdate) < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
)
)
and s_nationkey = n_nationkey
and n_name = 'CANADA'
order by
s_name;

--Query 20 normal --> ok 
select s_name,s_address 
from s_supplier, n_nation 
where s_suppkey in (
select ps_suppkey from ps_partsupp
where ps_partkey in (
select p_partkey from p_part
where p_name like 'forest%') and ps_availqty > (
select 0.5 * sum(l_quantity) 
from l_lineitem
where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' 
and l_shipdate < date '1994-01-01' + interval '1' year))
and s_nationkey = n_nationkey and n_name = 'CANADA'
order by s_name;

--Query 20 denormal


--Query 20 star hive ok
select
s_name,
s_address
from
s_supplier_star
where
s_suppkey in (
select
ps_suppkey
from
ps_partsupp_star
where
ps_partkey in (
select
p_partkey
from
p_part_star
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
lo_lineitem_orders_star
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= date('1994-01-01')
and l_shipdate < date('1994-01-01') + interval '1' year
)
)
and n_name = 'CANADA'
order by
s_name;

--Query 20 drill ok
select
s_name,
s_address
from
s_supplier_star
where
s_suppkey in (
select
ps_suppkey
from
ps_partsupp_star
where
ps_partkey in (
select
p_partkey
from
p_part_star
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
lo_lineitem_orders_star
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
)
)
and n_name = 'CANADA'
order by
s_name;

--Query 20 star impala ok
select
s_name,
s_address
from
s_supplier_star
where
s_suppkey in (
select
ps_suppkey
from
ps_partsupp_star
where
ps_partkey in (
select
p_partkey
from
p_part_star
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
lo_lineitem_orders_star
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= to_date('1994-01-01')
and l_shipdate < to_date(date_add(cast('1994-01-01' as timestamp), interval 1 year))
)
)
and n_name = 'CANADA'
order by
s_name;

--Query 21 raw / presto / drill /impala ok
select
s_name,
count(*) as numwait
from
s_supplier,
l_lineitem l1,
o_orders,
n_nation
where
s_suppkey = l1.l_suppkey
and o_orderkey = l1.l_orderkey
and o_orderstatus = 'F'
and l1.l_receiptdate > l1.l_commitdate
and exists (
select
*
from
l_lineitem l2
where
l2.l_orderkey = l1.l_orderkey
and l2.l_suppkey <> l1.l_suppkey
)
and not exists (
select
*
from
l_lineitem l3
where
l3.l_orderkey = l1.l_orderkey
and l3.l_suppkey <> l1.l_suppkey
and l3.l_receiptdate > l3.l_commitdate
)
and s_nationkey = n_nationkey
and n_name = 'SAUDI ARABIA'
group by
s_name
order by
numwait desc,
s_name limit 100;

--Query 21 normal --> ok --> angepasst
create temporary table l3 stored as orc as 
select l_orderkey, count(distinct l_suppkey) as cntSupp
from l_lineitem
where l_receiptdate > l_commitdate and l_orderkey is not null
group by l_orderkey
having cntSupp = 1;

set hive.auto.convert.join=false;
with location as (
select s_supplier.* from s_supplier, n_nation where
s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'
)
select s_name, count(*) as numwait
from
(
select li.l_suppkey, li.l_orderkey
from l_lineitem li join o_orders o on li.l_orderkey = o.o_orderkey and
      o.o_orderstatus = 'F'
     join
     (
     select l_orderkey, count(distinct l_suppkey) as cntSupp
     from l_lineitem
     group by l_orderkey
     ) l2 on li.l_orderkey = l2.l_orderkey and 
             li.l_receiptdate > li.l_commitdate and 
             l2.cntSupp > 1
) l1 join l3 on l1.l_orderkey = l3.l_orderkey
 join location s on l1.l_suppkey = s.s_suppkey
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;
set hive.auto.convert.join=true;

--Query 21 denormal impala hive drill ok
select
s_name,
count(*) as numwait
from
denormalized l1
where 
o_orderstatus = 'F'
and l1.l_receiptdate > l1.l_commitdate
and exists (
select
*
from
denormalized l2
where
l2.l_orderkey = l1.l_orderkey
and l2.l_suppkey <> l1.l_suppkey
)
and not exists (
select
*
from
denormalized l3
where
l3.l_orderkey = l1.l_orderkey
and l3.l_suppkey <> l1.l_suppkey
and l3.l_receiptdate > l3.l_commitdate
)
and s_nationkey = n2_nationkey
and n2_name = 'SAUDI ARABIA'
group by
s_name
order by
numwait desc,
s_name
limit 100;

--Query 21 star impala/drill ok --> hive not supported due to subquery
select
s_name,
count(*) as numwait
from
s_supplier_star,
lo_lineitem_orders_star l1
where
s_suppkey = l1.l_suppkey and 
l1.o_orderstatus = 'F'
and l1.l_receiptdate > l1.l_commitdate
and exists (
select
*
from
lo_lineitem_orders_star l2
where
l2.l_orderkey = l1.l_orderkey
and l2.l_suppkey <> l1.l_suppkey
)
and not exists (
select
*
from
lo_lineitem_orders_star l3
where
l3.l_orderkey = l1.l_orderkey
and l3.l_suppkey <> l1.l_suppkey
and l3.l_receiptdate > l3.l_commitdate
) and 
n_name = 'SAUDI ARABIA'
group by
s_name
order by
numwait desc,
s_name limit 100;

--Query 22 raw / presto / drill ok
select
cntrycode,
count(*) as numcust,
sum(c_acctbal) as totacctbal
from (
select
substring(c_phone from 1 for 2) as cntrycode,
c_acctbal
from
c_customer
where
substring (c_phone from 1 for 2) in
('13','31','23','29','30','18','17')
and c_acctbal > (
select
avg(c_acctbal)
from
c_customer
where
c_acctbal > 0.00
and substring (c_phone from 1 for 2) in
('13','31','23','29','30','18','17')
)
and not exists (
select
*
from
o_orders
where
o_custkey = c_custkey
)
) as custsale
group by
cntrycode
order by
cntrycode;

--Query 22 impala ok
select
cntrycode,
count(*) as numcust,
sum(c_acctbal) as totacctbal
from (
select
substring(c_phone, 1,2) as cntrycode,
c_acctbal
from
c_customer
where
substring (c_phone,1,2) in
('13','31','23','29','30','18','17')
and c_acctbal > (
select
avg(c_acctbal)
from
c_customer
where
c_acctbal > 0.00
and substring (c_phone,1,2) in
('13','31','23','29','30','18','17')
)
and not exists (
select
*
from
o_orders
where
o_custkey = c_custkey
)
) as custsale
group by
cntrycode
order by
cntrycode;

--Query 22 normal --> ok --> angepasst
create view if not exists q22_customer_tmp_cached as
select c_acctbal, c_custkey,substr(c_phone, 1, 2) as cntrycode
from c_customer
where substr(c_phone, 1, 2) = '13' or substr(c_phone, 1, 2) = '31' or substr(c_phone, 1, 2) = '23' or
substr(c_phone, 1, 2) = '29' or substr(c_phone, 1, 2) = '30' or substr(c_phone, 1, 2) = '18' or substr(c_phone, 1, 2) = '17';

create view if not exists q22_customer_tmp1_cached as
select avg(c_acctbal) as avg_acctbal 
from q22_customer_tmp_cached
where c_acctbal > 0.00;

create view if not exists q22_orders_tmp_cached as
select o_custkey
from o_orders
group by o_custkey;

set hive.strict.checks.cartesian.product=false;
select cntrycode, count(1) as numcust, sum(c_acctbal) as totacctbal
from (
    select cntrycode, c_acctbal,avg_acctbal 
from q22_customer_tmp1_cached ct1 inner join (
select cntrycode, c_acctbal 
from q22_orders_tmp_cached ot right outer join q22_customer_tmp_cached ct
on ct.c_custkey = ot.o_custkey
where
o_custkey is null
) ct2
) a
where c_acctbal > avg_acctbal
group by cntrycode
order by cntrycode;
set hive.strict.checks.cartesian.product=true;


--Query 22 denormal drill ok aber leichte abweichungen im ergebnis --> not working in hive Unsupported SubQuery Expression 'c_custkey': Only 1 SubQuery expression is supported
select
cntrycode,
count(*) as numcust,
sum(c_acctbal) as totacctbal
from (
select
substring(a.c_phone, 1,2) as cntrycode,
a.c_acctbal
from
denormalized a
where
 substring(a.c_phone, 1,2) in
('13','31','23','29','30','18','17')
and a.c_acctbal > (
select
avg(b.c_acctbal)
from
denormalized b
where
b.c_acctbal > 0.00
and substring(b.c_phone, 1,2) in
('13','31','23','29','30','18','17')
)
and not exists (
select
*
from
denormalized c
where
a.o_custkey = c.c_custkey
)
) as custsale
group by
cntrycode
order by
cntrycode;

--Query 22 denormal impala ok aber leichte abweichungen im ergebnis --> not working in hive Unsupported SubQuery Expression 'c_custkey': Only 1 SubQuery expression is supported
select
cntrycode,
count(*) as numcust,
sum(c_acctbal) as totacctbal
from (
select
substring(a.c_phone, 1,2) as cntrycode,
a.c_acctbal
from
denormalized a
where
 substring(a.c_phone, 1,2) in
('13','31','23','29','30','18','17')
and a.c_acctbal > (
select
avg(b.c_acctbal)
from
denormalized b
where
b.c_acctbal > 0.00
and 
 substring(b.c_phone, 1,2) in
('13','31','23','29','30','18','17')
)
        and not exists (
select
*
from
denormalized c
where
a.o_custkey = c.c_custkey
)
) as custsale
group by
cntrycode
order by
cntrycode;


--Query 22 star presto
select
cntrycode,
count(*) as numcust,
sum(c_acctbal) as totacctbal
from (
select
substring(c_phone from 1 for 2) as cntrycode,
c_acctbal
from
c_customer_star
where
substring (c_phone from 1 for 2) in
('13','31','23','29','30','18','17')
and c_acctbal > (
select
avg(c_acctbal)
from
c_customer_star
where
c_acctbal > 0.00
and substring (c_phone from 1 for 2) in
('13','31','23','29','30','18','17')
)
and not exists (
select
*
from
lo_lineitem_orders_star
where
o_custkey = c_custkey
)
) as custsale
group by
cntrycode
order by
cntrycode;

--Query 22 star  impala
select
cntrycode,
count(*) as numcust,
sum(c_acctbal) as totacctbal
from (
select
substring(c_phone , 1 , 2) as cntrycode,
c_acctbal
from
c_customer_star
where
substring (c_phone , 1 , 2) in
('13','31','23','29','30','18','17')
and c_acctbal > (
select
avg(c_acctbal)
from
c_customer_star
where
c_acctbal > 0.00
and substring (c_phone , 1 , 2) in
('13','31','23','29','30','18','17')
)
and not exists (
select
*
from
lo_lineitem_orders_star
where
o_custkey = c_custkey
)
) as custsale
group by
cntrycode
order by
cntrycode;