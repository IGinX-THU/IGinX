SELECT
    SUM( lineitem.l_extendedprice *( 1 - lineitem.l_discount )) AS revenue
FROM
    lineitem
JOIN part ON
    part.p_partkey = lineitem.l_partkey
WHERE
    (
        part.p_brand = 'Brand#12'
        AND(
            part.p_container = 'SM CASE'
            OR part.p_container = 'SM BOX'
            OR part.p_container = 'SM PACK'
            OR part.p_container = 'SM PKG'
        )
        AND lineitem.l_quantity >= 1
        AND lineitem.l_quantity <= 11
        AND part.p_size >= 1
        AND part.p_size <= 5
        AND(
            lineitem.l_shipmode = 'AIR'
            OR lineitem.l_shipmode = 'AIR REG'
        )
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR(
        part.p_brand = 'Brand#23'
        AND(
            part.p_container = 'MED PKG'
            OR part.p_container = 'MED BOX'
            OR part.p_container = 'MED BAG'
            OR part.p_container = 'MED PACK'
        )
        AND lineitem.l_quantity >= 10
        AND lineitem.l_quantity <= 20
        AND part.p_size >= 1
        AND part.p_size <= 10
        AND(
            lineitem.l_shipmode = 'AIR'
            OR lineitem.l_shipmode = 'AIR REG'
        )
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR(
        part.p_brand = 'Brand#34'
        AND(
            part.p_container = 'LG PACK'
            OR part.p_container = 'LG BOX'
            OR part.p_container = 'LG CASE'
            OR part.p_container = 'LG PKG'
        )
        AND lineitem.l_quantity >= 20
        AND lineitem.l_quantity <= 30
        AND part.p_size >= 1
        AND part.p_size <= 15
        AND(
            lineitem.l_shipmode = 'AIR'
            OR lineitem.l_shipmode = 'AIR REG'
        )
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    );
