SELECT
    lineitem.l_returnflag AS l_returnflag,
    lineitem.l_linestatus AS l_linestatus,
    SUM( lineitem.l_quantity ) AS sum_qty,
    SUM( lineitem.l_extendedprice ) AS sum_base_price,
    SUM( tmp1 ) AS sum_disc_price,
    SUM( tmp2 ) AS sum_charge,
    AVG( lineitem.l_quantity ) AS avg_qty,
    AVG( lineitem.l_extendedprice ) AS avg_price,
    AVG( lineitem.l_discount ) AS avg_disc,
    COUNT( lineitem.l_returnflag ) AS count_order
FROM
    (
        SELECT
            l_returnflag,
            l_linestatus,
            l_quantity,
            l_extendedprice,
            l_discount,
            l_extendedprice *(
                1 - l_discount
            ) AS tmp1,
            l_extendedprice *(
                1 - l_discount
            )*(
                1 + l_tax
            ) AS tmp2
        FROM
            lineitem
        WHERE
            lineitem.l_shipdate <= 904694400000
    )
GROUP BY
    lineitem.l_returnflag,
    lineitem.l_linestatus
ORDER BY
    lineitem.l_returnflag,
    lineitem.l_linestatus;