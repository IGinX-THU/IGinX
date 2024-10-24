WITH revenue AS(
    SELECT
        l_suppkey AS supplier_no,
        SUM( l_extendedprice *( 1 - l_discount )) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= 820425600000
        AND l_shipdate < 828288000000
    GROUP BY
        l_suppkey
) SELECT
    supplier.s_suppkey,
    supplier.s_name,
    supplier.s_address,
    supplier.s_phone,
    revenue.total_revenue
FROM
    supplier,
    revenue
WHERE
    supplier.s_suppkey = revenue.supplier_no
    AND revenue.total_revenue =(
        SELECT
            MAX( total_revenue )
        FROM
            revenue
    )
ORDER BY
    supplier.s_suppkey;