insert into tmpTableC(key, orderkey, year) values (
  select o_orderkey, extractYear(o_orderdate) from orders
);

select * from (
  select
      nation,
      o_year,
      sum(amount) as sum_profit
  from (
           select
               nation.n_name as nation,
               tmpTableC.year as o_year,
               lineitem.l_extendedprice * (1 - lineitem.l_discount) - partsupp.ps_supplycost * lineitem.l_quantity as amount
           from
               part
                   join lineitem on part.p_partkey = lineitem.l_partkey
                   join supplier on supplier.s_suppkey = lineitem.l_suppkey
                   join partsupp on partsupp.ps_suppkey = lineitem.l_suppkey and partsupp.ps_partkey = lineitem.l_partkey
                   join orders on orders.o_orderkey = lineitem.l_orderkey
                   join nation on supplier.s_nationkey = nation.n_nationkey
                   join tmpTableC on orders.o_orderkey = tmpTableC.orderkey
           where
                   part.p_name like '.*green.*'
       )
  group by
      o_year,
      nation
  order by
      o_year desc
)
order by nation;