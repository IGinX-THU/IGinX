select
    p_brand,
    p_type,
    p_size,
    supplier_cnt
from (
    select
        part.p_brand as p_brand,
        part.p_type as p_type,
        part.p_size as p_size,
        count(distinct partsupp.ps_suppkey) as supplier_cnt
    from
        partsupp
        join part on part.p_partkey = partsupp.ps_partkey
    where
        part.p_brand <> 'Brand#45'
        and part.p_partkey not in(
            select p_partkey
            from part
            where part.p_type like '.*MEDIUM POLISHED.*'
        )
        and (
            part.p_size = 3
            or part.p_size = 9
            or part.p_size = 14
            or part.p_size = 19
            or part.p_size = 23
            or part.p_size = 36
            or part.p_size = 45
            or part.p_size = 49
        )
        and partsupp.ps_suppkey not in (
            select s_suppkey
            from supplier
            where supplier.s_comment like '.*Customer.*Complaints.*'
        )
    group by
        part.p_brand,
        part.p_type,
        part.p_size
    order by
        part.p_brand,
        part.p_type,
        part.p_size
)
order by supplier_cnt desc;