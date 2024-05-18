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
                      mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp
                  from
                      postgres.customer
                          join mongotpch.orders on postgres.customer.c_custkey = mongotpch.orders.o_custkey
                          join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey
                          join postgres.supplier on mongotpch.lineitem.l_suppkey = postgres.supplier.s_suppkey and postgres.customer.c_nationkey = postgres.supplier.s_nationkey
                          join nation on postgres.supplier.s_nationkey = nation.n_nationkey
                          join postgres.region on nation.n_regionkey = postgres.region.r_regionkey
                  where
                          postgres.region.r_name = "ASIA"
                    and mongotpch.orders.o_orderdate >= 757353600000
                    and mongotpch.orders.o_orderdate < 788889600000 + 28800000
              )
         group by
             nation.n_name
     )
order by
    revenue desc;