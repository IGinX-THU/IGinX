select
    customer.c_name,
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_orderdate,
    orders.o_totalprice,
    sum(lineitem.l_quantity)
from
    customer
    join orders on customer.c_custkey = orders.o_custkey
    join lineitem on orders.o_orderkey = lineitem.l_orderkey
where orders.o_orderkey in (
    select lineitem.l_orderkey
    from (
        select
            l_orderkey,
            sum(l_quantity)
        from lineitem
        group by l_orderkey
        having sum(lineitem.l_quantity) > 300
    )
)
group by
    customer.c_name,
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_orderdate,
    orders.o_totalprice
order by orders.o_totalprice desc
limit 100;