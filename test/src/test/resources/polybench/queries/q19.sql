select
    sum(tmp) as revenue
from (
         select
                 mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) as tmp
         from
             mongotpch.lineitem
                 join tpchdata.part on tpchdata.part.p_partkey = mongotpch.lineitem.l_partkey
         where (
                     tpchdata.part.p_brand = 'Brand#12'
                 and (
                                 tpchdata.part.p_container = 'SM CASE'
                             or tpchdata.part.p_container = 'SM BOX'
                             or tpchdata.part.p_container = 'SM PACK'
                             or tpchdata.part.p_container = 'SM PKG'
                         )
                 and mongotpch.lineitem.l_quantity >= 1 and mongotpch.lineitem.l_quantity <= 11
                 and tpchdata.part.p_size >= 1 and tpchdata.part.p_size <= 5
                 and (mongotpch.lineitem.l_shipmode = 'AIR' or mongotpch.lineitem.l_shipmode = 'AIR REG')
                 and mongotpch.lineitem.l_shipinstruct = 'DELIVER IN PERSON'
             )
            or (
                     tpchdata.part.p_brand = 'Brand#23'
                 and (
                                 tpchdata.part.p_container = 'MED PKG'
                             or tpchdata.part.p_container = 'MED BOX'
                             or tpchdata.part.p_container = 'MED BAG'
                             or tpchdata.part.p_container = 'MED PACK'
                         )
                 and mongotpch.lineitem.l_quantity >= 10 and mongotpch.lineitem.l_quantity <= 20
                 and tpchdata.part.p_size >= 1 and tpchdata.part.p_size <= 10
                 and (mongotpch.lineitem.l_shipmode = 'AIR' or mongotpch.lineitem.l_shipmode = 'AIR REG')
                 and mongotpch.lineitem.l_shipinstruct = 'DELIVER IN PERSON'
             )
            or (
                     tpchdata.part.p_brand = 'Brand#34'
                 and (
                                 tpchdata.part.p_container = 'LG PACK'
                             or tpchdata.part.p_container = 'LG BOX'
                             or tpchdata.part.p_container = 'LG CASE'
                             or tpchdata.part.p_container = 'LG PKG'
                         )
                 and mongotpch.lineitem.l_quantity >= 20 and mongotpch.lineitem.l_quantity <= 30
                 and tpchdata.part.p_size >= 1 and tpchdata.part.p_size <= 15
                 and (mongotpch.lineitem.l_shipmode = 'AIR' or mongotpch.lineitem.l_shipmode = 'AIR REG')
                 and mongotpch.lineitem.l_shipinstruct = 'DELIVER IN PERSON'
             )
     );