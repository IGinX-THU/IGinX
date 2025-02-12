SELECT
    o_year,
    SUM( CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0.0 END )/ SUM( volume ) AS mkt_share
FROM
    (
        SELECT
            EXTRACT(
                orders.o_orderdate,
                "year"
            ) AS o_year,
            lineitem.l_extendedprice *(
                1 - lineitem.l_discount
            ) AS volume,
            n2.n_name AS nation
        FROM
            part
        JOIN lineitem ON
            part.p_partkey = lineitem.l_partkey
        JOIN supplier ON
            supplier.s_suppkey = lineitem.l_suppkey
        JOIN orders ON
            lineitem.l_orderkey = orders.o_orderkey
        JOIN customer ON
            orders.o_custkey = customer.c_custkey
        JOIN nation n1 ON
            customer.c_nationkey = n1.n_nationkey
        JOIN nation n2 ON
            supplier.s_nationkey = n2.n_nationkey
        JOIN region ON
            n1.n_regionkey = region.r_regionkey
        WHERE
            region.r_name = 'AMERICA'
            AND orders.o_orderdate >= 788889600000
            AND orders.o_orderdate <= 851961600000
            AND part.p_type = 'ECONOMY ANODIZED STEEL'
    )
GROUP BY
    o_year
ORDER BY
    o_year;