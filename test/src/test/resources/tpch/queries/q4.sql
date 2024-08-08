select
    o_orderpriority,
    count(o_orderkey) as order_count
from orders
where
    o_orderdate >= 741456000000
    and o_orderdate < 749404800000
    and exists (
        select l_orderkey
        from lineitem
        where
            lineitem.l_orderkey = orders.o_orderkey
            and lineitem.l_commitdate < lineitem.l_receiptdate
    )
group by o_orderpriority
order by o_orderpriority;