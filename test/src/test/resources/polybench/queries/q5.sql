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
                      tpchdata.customer
                          join mongotpch.orders on tpchdata.customer.c_custkey = mongotpch.orders.o_custkey
                          join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey
                          join tpchdata.supplier on mongotpch.lineitem.l_suppkey = tpchdata.supplier.s_suppkey and tpchdata.customer.c_nationkey = tpchdata.supplier.s_nationkey
                          join nation on tpchdata.supplier.s_nationkey = nation.n_nationkey
                          join tpchdata.region on nation.n_regionkey = tpchdata.region.r_regionkey
                  where
                          tpchdata.region.r_name = "ASIA"
                    and mongotpch.orders.o_orderdate >= 757353600000
                    and mongotpch.orders.o_orderdate < 788889600000
              )
         group by
             nation.n_name
     )
order by
    revenue desc;