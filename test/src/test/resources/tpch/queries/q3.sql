SELECT
    l_orderkey,
    SUM( tmp ) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    (
        SELECT
            lineitem.l_extendedprice *(
                1 - lineitem.l_discount
            ) AS tmp,
            lineitem.l_orderkey AS l_orderkey,
            orders.o_orderdate AS o_orderdate,
            orders.o_shippriority AS o_shippriority
        FROM
            customer
        JOIN orders ON
            customer.c_custkey = orders.o_custkey
        JOIN lineitem ON
            lineitem.l_orderkey = orders.o_orderkey
        WHERE
            customer.c_mktsegment = 'BUILDING'
            AND orders.o_orderdate < 795196800000
            AND lineitem.l_shipdate > 795225600000
    )
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC LIMIT 10;
