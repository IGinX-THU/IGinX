INSERT
    INTO
        tmpTable(
            KEY,
            p_key,
            minCost
        )
    VALUES(
        SELECT
            part.p_partkey AS p_key,
            MIN( partsupp.ps_supplycost ) AS minCost
        FROM
            partsupp
        JOIN supplier ON
            supplier.s_suppkey = partsupp.ps_suppkey
        JOIN nation ON
            supplier.s_nationkey = nation.n_nationkey
        JOIN region ON
            nation.n_regionkey = region.r_regionkey
        JOIN part ON
            part.p_partkey = partsupp.ps_partkey
        WHERE
            region.r_name = 'EUROPE'
            AND part.p_size = 15
            AND part.p_type LIKE "^.*BRASS"
        GROUP BY
            part.p_partkey
    );

SELECT
    supplier.s_acctbal AS s_acctbal,
    supplier.s_name AS s_name,
    nation.n_name AS n_name,
    part.p_partkey AS p_partkey,
    part.p_mfgr AS p_mfgr,
    supplier.s_address AS s_address,
    supplier.s_phone AS s_phone,
    supplier.s_comment AS s_comment
FROM
    part
JOIN partsupp ON
    part.p_partkey = partsupp.ps_partkey
JOIN supplier ON
    supplier.s_suppkey = partsupp.ps_suppkey
JOIN nation ON
    supplier.s_nationkey = nation.n_nationkey
JOIN region ON
    nation.n_regionkey = region.r_regionkey
JOIN tmpTable ON
    tmpTable.p_key = part.p_partkey
    AND partsupp.ps_supplycost = tmpTable.minCost
WHERE
    part.p_size = 15
    AND region.r_name = 'EUROPE'
    AND part.p_type LIKE "^.*BRASS"
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey LIMIT 100;
