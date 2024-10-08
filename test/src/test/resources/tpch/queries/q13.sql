SELECT
    c_count,
    COUNT( c_custkey ) AS custdist
FROM
    (
        SELECT
            customer.c_custkey AS c_custkey,
            COUNT( orders.o_orderkey ) AS c_count
        FROM
            customer
        LEFT OUTER JOIN orders ON
            customer.c_custkey = orders.o_custkey
            AND orders.o_comment NOT LIKE '.*special.*requests.*'
        GROUP BY
            customer.c_custkey
    )
GROUP BY
    c_count
ORDER BY -- spotless:off
    `count(c_custkey)` DESC, -- spotless:on
    c_count DESC;