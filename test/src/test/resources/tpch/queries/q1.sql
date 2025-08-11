SELECT
    l_returnflag,
    l_linestatus,
    SUM( l_quantity ) AS sum_qty,
    SUM( l_extendedprice ) AS sum_base_price,
    SUM( l_extendedprice*( 1 - l_discount )) AS sum_disc_price,
    SUM( l_extendedprice*( 1 - l_discount )*( 1 + l_tax )) AS sum_charge,
    AVG( l_quantity ) AS avg_qty,
    AVG( l_extendedprice ) AS avg_price,
    AVG( l_discount ) AS avg_disc,
    COUNT( l_returnflag ) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= 904694400000
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
