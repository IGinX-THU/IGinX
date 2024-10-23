SELECT
    customer.c_name,
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_orderdate,
    orders.o_totalprice,
    SUM( lineitem.l_quantity )
FROM
    customer
JOIN orders ON
    customer.c_custkey = orders.o_custkey
JOIN lineitem ON
    orders.o_orderkey = lineitem.l_orderkey
WHERE
    orders.o_orderkey IN(
        SELECT
            lineitem.l_orderkey
        FROM
            (
                SELECT
                    l_orderkey,
                    SUM( l_quantity )
                FROM
                    lineitem
                GROUP BY
                    l_orderkey
                HAVING
                    SUM( l_quantity )> 300
            )
    )
GROUP BY
    customer.c_name,
    customer.c_custkey,
    orders.o_orderkey,
    orders.o_orderdate,
    orders.o_totalprice
ORDER BY
    orders.o_totalprice DESC LIMIT 100;