select
    supplier.s_name,
    count(supplier.s_name) as numwait
from
    lineitem as l1
    join supplier on supplier.s_suppkey = l1.l_suppkey
    join orders on orders.o_orderkey = l1.l_orderkey
    join nation on supplier.s_nationkey = nation.n_nationkey
where
    orders.o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select l_orderkey
        from lineitem as l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select l_orderkey
        from lineitem as l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and nation.n_name = 'SAUDI ARABIA'
group by supplier.s_name
order by
    `count(supplier.s_name)` desc,
    supplier.s_name
limit 100;