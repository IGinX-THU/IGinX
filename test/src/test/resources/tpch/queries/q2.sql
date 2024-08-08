insert into tmpTable(key, p_key, minCost) values (
    select
        part.p_partkey as p_key,
        min(partsupp.ps_supplycost) as minCost
    from
        partsupp
        join supplier on supplier.s_suppkey = partsupp.ps_suppkey
        join nation on supplier.s_nationkey = nation.n_nationkey
        join region on nation.n_regionkey = region.r_regionkey
        join part on part.p_partkey = partsupp.ps_partkey
    where
        region.r_name = 'EUROPE'
        and part.p_size = 15
        and part.p_type like "^.*BRASS"
    group by part.p_partkey
);

select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from (
    select
        supplier.s_acctbal as s_acctbal,
        supplier.s_name as s_name,
        nation.n_name as n_name,
        part.p_partkey as p_partkey,
        part.p_mfgr as p_mfgr,
        supplier.s_address as s_address,
        supplier.s_phone as s_phone,
        supplier.s_comment as s_comment
    from
        part
        join partsupp on part.p_partkey = partsupp.ps_partkey
        join supplier on supplier.s_suppkey = partsupp.ps_suppkey
        join nation on supplier.s_nationkey = nation.n_nationkey
        join region on nation.n_regionkey = region.r_regionkey
        join tmpTable on tmpTable.p_key = part.p_partkey and partsupp.ps_supplycost = tmpTable.minCost
    where
        part.p_size = 15
        and region.r_name = 'EUROPE'
        and part.p_type like "^.*BRASS"
    order by
        nation.n_name,
        supplier.s_name,
        part.p_partkey
)
order by s_acctbal desc
limit 100;