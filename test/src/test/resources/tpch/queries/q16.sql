SELECT
    p_brand,
    p_type,
    p_size,
    supplier_cnt
FROM
    (
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
            AND part.p_partkey NOT IN(
                SELECT
                    p_partkey
                FROM
                    part AS p
                WHERE
                    p.p_type LIKE '.*MEDIUM POLISHED.*'
            )
            AND(
                part.p_size = 3
                OR part.p_size = 9
                OR part.p_size = 14
                OR part.p_size = 19
                OR part.p_size = 23
                OR part.p_size = 36
                OR part.p_size = 45
                OR part.p_size = 49
            )
            AND partsupp.ps_suppkey NOT IN(
                SELECT
                    s_suppkey
                FROM
                    supplier
                WHERE
                    supplier.s_comment LIKE '.*Customer.*Complaints.*'
            )
        GROUP BY
            part.p_brand,
            part.p_type,
            part.p_size
        ORDER BY
            part.p_brand,
            part.p_type,
            part.p_size
    )
ORDER BY
    supplier_cnt DESC;