insert into tmpTableB(key, p_partkey, val) values (
    select
        part.p_partkey,
        0.2 * tmp
    from (
        select
            part.p_partkey,
            avg(lineitem.l_quantity) as tmp
        from
            lineitem
            join part on lineitem.l_partkey = part.p_partkey
        group by part.p_partkey
    )
);

select
    tmp2 / 7 as avg_yearly
from (
    select sum(lineitem.l_extendedprice) as tmp2
    from
        lineitem
        join part on part.p_partkey = lineitem.l_partkey
        join tmpTableB on tmpTableB.p_partkey = lineitem.l_partkey
    where
        part.p_brand = 'Brand#23'
        and part.p_container = 'MED BOX'
        and lineitem.l_quantity < tmpTableB.val
);