WITH tmpTableB AS(
    SELECT
        part.p_partkey AS p_partkey,
        0.2 * tmp AS val
    FROM
        (
            SELECT
                part.p_partkey,
                AVG( lineitem.l_quantity ) AS tmp
            FROM
                lineitem
            JOIN part ON
                lineitem.l_partkey = part.p_partkey
            GROUP BY
                part.p_partkey
        )
) SELECT
    tmp2 / 7 AS avg_yearly
FROM
    (
        SELECT
            SUM( lineitem.l_extendedprice ) AS tmp2
        FROM
            lineitem
        JOIN part ON
            part.p_partkey = lineitem.l_partkey
        JOIN tmpTableB ON
            tmpTableB.p_partkey = lineitem.l_partkey
        WHERE
            part.p_brand = 'Brand#23'
            AND part.p_container = 'MED BOX'
            AND lineitem.l_quantity < tmpTableB.val
    );