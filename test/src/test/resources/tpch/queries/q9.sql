INSERT
    INTO
        tmpTableC(
            KEY,
            orderkey,
            YEAR
        )
    VALUES(
        SELECT
            o_orderkey,
            extractYear(o_orderdate)
        FROM
            orders
    );

SELECT
    nation,
    o_year,
    SUM( amount ) AS sum_profit
FROM
    (
        SELECT
            nation.n_name AS nation,
            tmpTableC.year AS o_year,
            lineitem.l_extendedprice *(
                1 - lineitem.l_discount
            )- partsupp.ps_supplycost * lineitem.l_quantity AS amount
        FROM
            part
        JOIN lineitem ON
            part.p_partkey = lineitem.l_partkey
        JOIN supplier ON
            supplier.s_suppkey = lineitem.l_suppkey
        JOIN partsupp ON
            partsupp.ps_suppkey = lineitem.l_suppkey
            AND partsupp.ps_partkey = lineitem.l_partkey
        JOIN orders ON
            orders.o_orderkey = lineitem.l_orderkey
        JOIN nation ON
            supplier.s_nationkey = nation.n_nationkey
        JOIN tmpTableC ON
            orders.o_orderkey = tmpTableC.orderkey
        WHERE
            part.p_name LIKE '.*green.*'
    )
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;