select
    customer.c_custkey,
    customer.c_name,
    revenue,
    customer.c_acctbal,
    nation.n_name,
    customer.c_address,
    customer.c_phone,
    customer.c_comment
from (
         select
             customer.c_custkey,
             customer.c_name,
             sum(tmp) as revenue,
             customer.c_acctbal,
             nation.n_name,
             customer.c_address,
             customer.c_phone,
             customer.c_comment
         from (
                  select
                      customer.c_custkey,
                      customer.c_name,
                      lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp,
                      customer.c_acctbal,
                      nation.n_name,
                      customer.c_address,
                      customer.c_phone,
                      customer.c_comment
                  from
                      customer
                          join orders on customer.c_custkey = orders.o_custkey
                          join lineitem on lineitem.l_orderkey = orders.o_orderkey
                          join nation on customer.c_nationkey = nation.n_nationkey
                  where
                          orders.o_orderdate >= 749404800000
                    and orders.o_orderdate < 757353600000
                    and lineitem.l_returnflag = 'R'
              )
         group by
             customer.c_custkey,
             customer.c_name,
             customer.c_acctbal,
             customer.c_phone,
             nation.n_name,
             customer.c_address,
             customer.c_comment
     )
order by
    revenue desc
limit 20;