SELECT
    customer.c_custkey,
    customer.c_name,
    revenue,
    customer.c_acctbal,
    nation.n_name,
    customer.c_address,
    customer.c_phone,
    customer.c_comment
FROM
    (
        SELECT
            customer.c_custkey,
            customer.c_name,
            SUM( tmp ) AS revenue,
            customer.c_acctbal,
            nation.n_name,
            customer.c_address,
            customer.c_phone,
            customer.c_comment
        FROM
            (
                SELECT
                    customer.c_custkey,
                    customer.c_name,
                    lineitem.l_extendedprice *(
                        1 - lineitem.l_discount
                    ) AS tmp,
                    customer.c_acctbal,
                    nation.n_name,
                    customer.c_address,
                    customer.c_phone,
                    customer.c_comment
                FROM
                    customer
                JOIN orders ON
                    customer.c_custkey = orders.o_custkey
                JOIN lineitem ON
                    lineitem.l_orderkey = orders.o_orderkey
                JOIN nation ON
                    customer.c_nationkey = nation.n_nationkey
                WHERE
                    orders.o_orderdate >= 749404800000
                    AND orders.o_orderdate < 757353600000
                    AND lineitem.l_returnflag = 'R'
            )
        GROUP BY
            customer.c_custkey,
            customer.c_name,
            customer.c_acctbal,
            customer.c_phone,
            nation.n_name,
            customer.c_address,
            customer.c_comment
    )
ORDER BY
    revenue DESC LIMIT 20;