SELECT
    part.p_brand AS p_brand,
    part.p_type AS p_type,
    part.p_size AS p_size,
    COUNT( DISTINCT partsupp.ps_suppkey ) AS supplier_cnt
FROM
    partsupp
JOIN part ON
    part.p_partkey = partsupp.ps_partkey
WHERE
    part.p_brand <> 'Brand#45'
    AND part.p_size IN(
        3,
        9,
        14,
        19,
        23,
        36,
        45,
        49
    )
    AND part.p_type NOT LIKE '.*MEDIUM POLISHED.*'
    AND partsupp.ps_suppkey NOT IN(
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            supplier.s_comment LIKE '.*Customer.*Complaints.*'
    )
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;