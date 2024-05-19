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
                     mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp,
                     mongotpch.lineitem.l_orderkey as l_orderkey,
                     mongotpch.orders.o_orderdate as o_orderdate,
                     mongotpch.orders.o_shippriority as o_shippriority
             from
                 postgres.customer
                     join mongotpch.orders on postgres.customer.c_custkey = mongotpch.orders.o_custkey
                     join mongotpch.lineitem on mongotpch.lineitem.l_orderkey = mongotpch.orders.o_orderkey
             where
                     postgres.customer.c_mktsegment = 'BUILDING'
               and mongotpch.orders.o_orderdate < 795196800000
               and mongotpch.lineitem.l_shipdate > 795225600000
         )
     group by
         l_orderkey,
         o_orderdate,
         o_shippriority
    )
order by revenue desc
    limit 10;