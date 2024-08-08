insert into tmpTableA(key, partkey, suppkey, val) values (
    select
        partkey,
        suppkey,
        0.5 * tmp
    from (
        select
            l_partkey as partkey,
            l_suppkey as suppkey,
            sum(l_quantity) as tmp
        from lineitem
        where
            lineitem.l_shipdate >= 757353600000
            and lineitem.l_shipdate < 788889600000
        group by l_partkey, l_suppkey
    )
);

select
    supplier.s_name,
    supplier.s_address
from
    supplier
    join nation on supplier.s_nationkey = nation.n_nationkey
where
    supplier.s_suppkey in (
        select partsupp.ps_suppkey
        from
            partsupp
            join tmpTableA on tmpTableA.suppkey = partsupp.ps_suppkey and tmpTableA.partkey = partsupp.ps_partkey
        where
            partsupp.ps_partkey in (
            select p_partkey
            from part
            where part.p_name like 'forest.*'
        )
        and partsupp.ps_availqty > tmpTableA.val
    )
    and nation.n_name = 'CANADA'
order by supplier.s_name;