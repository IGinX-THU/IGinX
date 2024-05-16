select
    postgres.customer.c_name,
    postgres.customer.c_custkey,
    mongotpch.orders.o_orderkey,
    mongotpch.orders.o_orderdate,
    mongotpch.orders.o_totalprice,
    sum(mongotpch.lineitem.l_quantity)
from
    postgres.customer
        join mongotpch.orders on postgres.customer.c_custkey = mongotpch.orders.o_custkey
        join mongotpch.lineitem on mongotpch.orders.o_orderkey = mongotpch.lineitem.l_orderkey
where
        mongotpch.orders.o_orderkey in (
        select
            mongotpch.lineitem.l_orderkey
        from (
                 select
                     l_orderkey,
                     sum(l_quantity)
                 from
                     mongotpch.lineitem
                 group by
                     l_orderkey
                 having
                         sum(mongotpch.lineitem.l_quantity) > 300
             )
    )
group by
    postgres.customer.c_name,
    postgres.customer.c_custkey,
    mongotpch.orders.o_orderkey,
    mongotpch.orders.o_orderdate,
    mongotpch.orders.o_totalprice
order by
    mongotpch.orders.o_orderdate,
    mongotpch.orders.o_totalprice desc;