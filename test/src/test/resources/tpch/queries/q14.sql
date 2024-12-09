SELECT
    100.00 * SUM( CASE WHEN part.p_type LIKE 'PROMO.*' THEN lineitem.l_extendedprice *( 1 - lineitem.l_discount ) ELSE 0.0 END )/ SUM( lineitem.l_extendedprice *( 1 - lineitem.l_discount )) AS promo_revenue
FROM
    lineitem
JOIN part ON
    lineitem.l_partkey = part.p_partkey
WHERE
    lineitem.l_shipdate >= 809884800000
    AND lineitem.l_shipdate < 812476800000;