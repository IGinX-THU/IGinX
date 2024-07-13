select
    nation.n_name,
    revenue
from (
         select
             nation.n_name,
             sum(tmp) as revenue
         from (
                  select
                      nation.n_name,
                      lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp
                  from
                      customer
                          join orders on customer.c_custkey = orders.o_custkey
                          join lineitem on lineitem.l_orderkey = orders.o_orderkey
                          join supplier on lineitem.l_suppkey = supplier.s_suppkey and customer.c_nationkey = supplier.s_nationkey
                          join nation on supplier.s_nationkey = nation.n_nationkey
                          join region on nation.n_regionkey = region.r_regionkey
                  where
                          region.r_name = "ASIA"
                    and orders.o_orderdate >= 757353600000
                    and orders.o_orderdate < 788889600000
              )
         group by
             nation.n_name
     )
order by
    revenue desc;