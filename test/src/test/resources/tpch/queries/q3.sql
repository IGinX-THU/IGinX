select l_orderkey,
       revenue,
       o_orderdate,
       o_shippriority from
    (select
         l_orderkey,
         o_orderdate,
         o_shippriority,
         sum(tmp) as revenue
     from(
             select
                     lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp,
                     lineitem.l_orderkey as l_orderkey,
                     orders.o_orderdate as o_orderdate,
                     orders.o_shippriority as o_shippriority
             from
                 customer
                     join orders on customer.c_custkey = orders.o_custkey
                     join lineitem on lineitem.l_orderkey = orders.o_orderkey
             where
                     customer.c_mktsegment = 'BUILDING'
               and orders.o_orderdate < 795196800000
               and lineitem.l_shipdate > 795225600000
         )
     group by
         l_orderkey,
         o_orderdate,
         o_shippriority
    )
order by revenue desc
    limit 10;