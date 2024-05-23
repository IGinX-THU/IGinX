select
    tpchdata.customer.c_custkey,
    tpchdata.customer.c_name,
    revenue,
    tpchdata.customer.c_acctbal,
    nation.n_name,
    tpchdata.customer.c_address,
    tpchdata.customer.c_phone,
    tpchdata.customer.c_comment
from (
         select
             tpchdata.customer.c_custkey,
             tpchdata.customer.c_name,
             sum(tmp) as revenue,
             tpchdata.customer.c_acctbal,
             nation.n_name,
             tpchdata.customer.c_address,
             tpchdata.customer.c_phone,
             tpchdata.customer.c_comment
         from (
                  select
                      tpchdata.customer.c_custkey,
                      tpchdata.customer.c_name,
                      mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp,
                      tpchdata.customer.c_acctbal,
                      nation.n_name,
                      tpchdata.customer.c_address,
                      tpchdata.customer.c_phone,
                      tpchdata.customer.c_comment
                  from
                      tpchdata.customer
                          join mongotpch.orders on tpchdata.customer.c_custkey = mongotpch.orders.o_custkey
                          join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey
                          join nation on tpchdata.customer.c_nationkey = nation.n_nationkey
                  where
                          mongotpch.orders.o_orderdate >= 749404800000
                    and mongotpch.orders.o_orderdate < 757353600000
                    and mongotpch.lineitem.l_returnflag = 'R'
              )
         group by
             tpchdata.customer.c_custkey,
             tpchdata.customer.c_name,
             tpchdata.customer.c_acctbal,
             tpchdata.customer.c_phone,
             nation.n_name,
             tpchdata.customer.c_address,
             tpchdata.customer.c_comment
     )
order by
    revenue desc
limit 20;