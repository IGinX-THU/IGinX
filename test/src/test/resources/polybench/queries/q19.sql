select
    sum(tmp) as revenue
from (
         select
                 mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp
         from
             mongotpch.lineitem
                 join postgres.part on postgres.part.p_partkey = mongotpch.lineitem.l_partkey
         where (
                     postgres.part.p_brand = 'Brand#12'
                 and (
                                 postgres.part.p_container = 'SM CASE'
                             or postgres.part.p_container = 'SM BOX'
                             or postgres.part.p_container = 'SM PACK'
                             or postgres.part.p_container = 'SM PKG'
                         )
                 and mongotpch.lineitem.l_quantity >= 1 and mongotpch.lineitem.l_quantity <= 11
                 and postgres.part.p_size >= 1 and postgres.part.p_size <= 5
                 and (mongotpch.lineitem.l_shipmode = 'AIR' or mongotpch.lineitem.l_shipmode = 'AIR REG')
                 and mongotpch.lineitem.l_shipinstruct = 'DELIVER IN PERSON'
             )
            or (
                     postgres.part.p_brand = 'Brand#23'
                 and (
                                 postgres.part.p_container = 'MED PKG'
                             or postgres.part.p_container = 'MED BOX'
                             or postgres.part.p_container = 'MED BAG'
                             or postgres.part.p_container = 'MED PACK'
                         )
                 and mongotpch.lineitem.l_quantity >= 10 and mongotpch.lineitem.l_quantity <= 20
                 and postgres.part.p_size >= 1 and postgres.part.p_size <= 10
                 and (mongotpch.lineitem.l_shipmode = 'AIR' or mongotpch.lineitem.l_shipmode = 'AIR REG')
                 and mongotpch.lineitem.l_shipinstruct = 'DELIVER IN PERSON'
             )
            or (
                     postgres.part.p_brand = 'Brand#34'
                 and (
                                 postgres.part.p_container = 'LG PACK'
                             or postgres.part.p_container = 'LG BOX'
                             or postgres.part.p_container = 'LG CASE'
                             or postgres.part.p_container = 'LG PKG'
                         )
                 and mongotpch.lineitem.l_quantity >= 20 and mongotpch.lineitem.l_quantity <= 30
                 and postgres.part.p_size >= 1 and postgres.part.p_size <= 15
                 and (mongotpch.lineitem.l_shipmode = 'AIR' or mongotpch.lineitem.l_shipmode = 'AIR REG')
                 and mongotpch.lineitem.l_shipinstruct = 'DELIVER IN PERSON'
             )
     );