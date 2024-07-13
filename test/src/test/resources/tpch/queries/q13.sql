select
    c_count,
    custdist
from (
         select
             c_count,
             count(c_custkey) as custdist
         from (
                  select
                      customer.c_custkey as c_custkey,
                      count(orders.o_orderkey) as c_count
                  from
                      customer
                          left outer join orders
                                          on customer.c_custkey = orders.o_custkey
                                              and !(orders.o_comment like '.*pending.*')
                  group by
                      customer.c_custkey
              )
         group by
             c_count
     )
order by
    custdist,
    c_count desc;