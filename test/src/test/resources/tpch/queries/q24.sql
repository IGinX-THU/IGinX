SELECT
    SUM( lineitem.l_extendedprice *( 1 - lineitem.l_discount )) AS revenue
FROM
    lineitem
JOIN part ON
    part.p_partkey = lineitem.l_partkey
WHERE
    (
        part.p_brand = 'Brand#12'
        AND part.p_container IN(
            'SM CASE',
            'SM BOX',
            'SM PACK',
            'SM PKG'
        )
        AND lineitem.l_quantity >= 1
        AND lineitem.l_quantity <= 11
        AND part.p_size >= 1
        AND part.p_size <= 5
        AND lineitem.l_shipmode IN(
            'AIR',
            'AIR REG'
        )
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR(
        part.p_brand = 'Brand#23'
        AND part.p_container IN(
            'MED BAG',
            'MED BOX',
            'MED PKG',
            'MED PACK'
        )
        AND lineitem.l_quantity >= 10
        AND lineitem.l_quantity <= 20
        AND part.p_size >= 1
        AND part.p_size <= 10
        AND lineitem.l_shipmode IN(
            'AIR',
            'AIR REG'
        )
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR(
        part.p_brand = 'Brand#34'
        AND part.p_container IN(
            'LG CASE',
            'LG BOX',
            'LG PACK',
            'LG PKG'
        )
        AND lineitem.l_quantity >= 20
        AND lineitem.l_quantity <= 30
        AND part.p_size >= 1
        AND part.p_size <= 15
        AND lineitem.l_shipmode IN(
            'AIR',
            'AIR REG'
        )
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'
    );