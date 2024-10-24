SELECT
    partsupp.ps_partkey,
    SUM( partsupp.ps_supplycost * partsupp.ps_availqty ) AS val
FROM
    partsupp
JOIN supplier ON
    partsupp.ps_suppkey = supplier.s_suppkey
JOIN nation ON
    supplier.s_nationkey = nation.n_nationkey
WHERE
    nation.n_name = 'GERMANY'
GROUP BY
    partsupp.ps_partkey
HAVING
    SUM( partsupp.ps_supplycost * partsupp.ps_availqty )>(
        SELECT
            SUM( partsupp.ps_supplycost * partsupp.ps_availqty )* 0.0001000000
        FROM
            partsupp
        JOIN supplier ON
            partsupp.ps_suppkey = supplier.s_suppkey
        JOIN nation ON
            supplier.s_nationkey = nation.n_nationkey
        WHERE
            nation.n_name = 'GERMANY'
    )
ORDER BY
    val DESC;