select sum(tmp) as revenue
from (
    select lineitem.l_extendedprice * (1 - lineitem.l_discount) as tmp
    from
        lineitem
        join part on part.p_partkey = lineitem.l_partkey
    where (
            part.p_brand = 'Brand#12'
            and (
                part.p_container = 'SM CASE'
                or part.p_container = 'SM BOX'
                or part.p_container = 'SM PACK'
                or part.p_container = 'SM PKG'
            )
            and lineitem.l_quantity >= 1 and lineitem.l_quantity <= 11
            and part.p_size >= 1 and part.p_size <= 5
            and (lineitem.l_shipmode = 'AIR' or lineitem.l_shipmode = 'AIR REG')
            and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
        )
        or (
            part.p_brand = 'Brand#23'
            and (
                part.p_container = 'MED PKG'
                or part.p_container = 'MED BOX'
                or part.p_container = 'MED BAG'
                or part.p_container = 'MED PACK'
            )
            and lineitem.l_quantity >= 10 and lineitem.l_quantity <= 20
            and part.p_size >= 1 and part.p_size <= 10
            and (lineitem.l_shipmode = 'AIR' or lineitem.l_shipmode = 'AIR REG')
            and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
        )
        or (
            part.p_brand = 'Brand#34'
            and (
                part.p_container = 'LG PACK'
                or part.p_container = 'LG BOX'
                or part.p_container = 'LG CASE'
                or part.p_container = 'LG PKG'
            )
            and lineitem.l_quantity >= 20 and lineitem.l_quantity <= 30
            and part.p_size >= 1 and part.p_size <= 15
            and (lineitem.l_shipmode = 'AIR' or lineitem.l_shipmode = 'AIR REG')
            and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
        )
);