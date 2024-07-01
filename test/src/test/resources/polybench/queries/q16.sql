select
    p_brand,
    p_type,
    p_size,
    supplier_cnt
from (
         select
             tpchdata.part.p_brand as p_brand,
             tpchdata.part.p_type as p_type,
             tpchdata.part.p_size as p_size,
             count(distinct tpchdata.partsupp.ps_suppkey) as supplier_cnt
         from
             tpchdata.partsupp
                 join tpchdata.part on tpchdata.part.p_partkey = tpchdata.partsupp.ps_partkey
         where
                 tpchdata.part.p_brand <> 'Brand#45'
           and tpchdata.part.p_partkey not in(
             select p_partkey from tpchdata.part
             where tpchdata.part.p_type like '.*MEDIUM POLISHED.*'
         )
           and (
                     tpchdata.part.p_size = 3
                 or tpchdata.part.p_size = 9
                 or tpchdata.part.p_size = 14
                 or tpchdata.part.p_size = 19
                 or tpchdata.part.p_size = 23
                 or tpchdata.part.p_size = 36
                 or tpchdata.part.p_size = 45
                 or tpchdata.part.p_size = 49
             )
           and tpchdata.partsupp.ps_suppkey not in (
             select
                 s_suppkey
             from
                 tpchdata.supplier
             where
                     tpchdata.supplier.s_comment like '.*Customer.*Complaints.*'
         )
         group by
             tpchdata.part.p_brand,
             tpchdata.part.p_type,
             tpchdata.part.p_size
         order by
             tpchdata.part.p_brand,
             tpchdata.part.p_type,
             tpchdata.part.p_size
     )
order by
    supplier_cnt desc;