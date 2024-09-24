SELECT
    o_orderpriority,
    COUNT( o_orderkey ) AS order_count
FROM
    orders
WHERE
    o_orderdate >= 741456000000
    AND o_orderdate < 749404800000
    AND EXISTS(
        SELECT
            l_orderkey
        FROM
            lineitem
        WHERE
            lineitem.l_orderkey = orders.o_orderkey
            AND lineitem.l_commitdate < lineitem.l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;