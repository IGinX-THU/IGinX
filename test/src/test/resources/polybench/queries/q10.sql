select
    postgres.customer.c_custkey,
    postgres.customer.c_name,
    revenue,
    postgres.customer.c_acctbal,
    nation.n_name,
    postgres.customer.c_address,
    postgres.customer.c_phone,
    postgres.customer.c_comment
from (
         select
             postgres.customer.c_custkey,
             postgres.customer.c_name,
             sum(tmp) as revenue,
             postgres.customer.c_acctbal,
             nation.n_name,
             postgres.customer.c_address,
             postgres.customer.c_phone,
             postgres.customer.c_comment
         from (
                  select
                      postgres.customer.c_custkey,
                      postgres.customer.c_name,
                      mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp,
                      postgres.customer.c_acctbal,
                      nation.n_name,
                      postgres.customer.c_address,
                      postgres.customer.c_phone,
                      postgres.customer.c_comment
                  from
                      postgres.customer
                          join mongotpch.orders on postgres.customer.c_custkey = mongotpch.orders.o_custkey
                          join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey
                          join nation on postgres.customer.c_nationkey = nation.n_nationkey
                  where
                          mongotpch.orders.o_orderdate >= 749404800000
                    and mongotpch.orders.o_orderdate < 757353600000
                    and mongotpch.lineitem.l_returnflag = 'R'
              )
         group by
             postgres.customer.c_custkey,
             postgres.customer.c_name,
             postgres.customer.c_acctbal,
             postgres.customer.c_phone,
             nation.n_name,
             postgres.customer.c_address,
             postgres.customer.c_comment
     )
order by
    revenue desc
limit 20;