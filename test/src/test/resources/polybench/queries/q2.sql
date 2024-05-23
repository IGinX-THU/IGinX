insert into tmpTable(key, p_key, minCost) values (
select
     tpchdata.part.p_partkey as p_key,
     min(tpchdata.partsupp.ps_supplycost) as minCost
from
    tpchdata.partsupp
    join tpchdata.supplier on tpchdata.supplier.s_suppkey = tpchdata.partsupp.ps_suppkey
    join nation on tpchdata.supplier.s_nationkey = nation.n_nationkey
    join tpchdata.region on nation.n_regionkey = tpchdata.region.r_regionkey
    join tpchdata.part on tpchdata.part.p_partkey = tpchdata.partsupp.ps_partkey
where
    tpchdata.region.r_name = 'EUROPE'
    and tpchdata.part.p_size = 15
    and tpchdata.part.p_type like "^.*BRASS"
group by tpchdata.part.p_partkey
);

select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from
    (select
         tpchdata.supplier.s_acctbal as s_acctbal,
         tpchdata.supplier.s_name as s_name,
         nation.n_name as n_name,
         tpchdata.part.p_partkey as p_partkey,
         tpchdata.part.p_mfgr as p_mfgr,
         tpchdata.supplier.s_address as s_address,
         tpchdata.supplier.s_phone as s_phone,
         tpchdata.supplier.s_comment as s_comment
     from
         tpchdata.part
             join tpchdata.partsupp on tpchdata.part.p_partkey = tpchdata.partsupp.ps_partkey
             join tpchdata.supplier on tpchdata.supplier.s_suppkey = tpchdata.partsupp.ps_suppkey
             join nation on tpchdata.supplier.s_nationkey = nation.n_nationkey
             join tpchdata.region on nation.n_regionkey = tpchdata.region.r_regionkey
             join tmpTable on tmpTable.p_key = tpchdata.part.p_partkey and tpchdata.partsupp.ps_supplycost = tmpTable.minCost
     where
             tpchdata.part.p_size = 15
       and tpchdata.region.r_name = 'EUROPE'
       and tpchdata.part.p_type like "^.*BRASS"
     order by
         nation.n_name,
         tpchdata.supplier.s_name,
         tpchdata.part.p_partkey
    )
order by s_acctbal desc;