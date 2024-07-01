select
    c_count,
    custdist
from (
         select
             c_count,
             count(c_custkey) as custdist
         from (
                  select
                      tpchdata.customer.c_custkey as c_custkey,
                      count(mongotpch.orders.o_orderkey) as c_count
                  from
                      tpchdata.customer
                          left outer join mongotpch.orders
                                          on tpchdata.customer.c_custkey = mongotpch.orders.o_custkey
                                              and !(mongotpch.orders.o_comment like '.*pending.*')
                  group by
                      tpchdata.customer.c_custkey
              )
         group by
             c_count
     )
order by
    custdist,
    c_count desc;