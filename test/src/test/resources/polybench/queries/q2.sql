insert into tmpTable(key, p_key, minCost) values (select
                                                     postgres.part.p_partkey as p_key,
                                                     min(postgres.partsupp.ps_supplycost) as minCost
        from
            postgres.partsupp
            join postgres.supplier on postgres.supplier.s_suppkey = postgres.partsupp.ps_suppkey
            join nation on postgres.supplier.s_nationkey = nation.n_nationkey
            join postgres.region on nation.n_regionkey = postgres.region.r_regionkey
            join postgres.part on postgres.part.p_partkey = postgres.partsupp.ps_partkey
        where
						postgres.region.r_name = 'EUROPE'
						and postgres.part.p_size = 15
						and postgres.part.p_type like "^.*BRASS"
				group by postgres.part.p_partkey);


select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from
    (select
         postgres.supplier.s_acctbal as s_acctbal,
         postgres.supplier.s_name as s_name,
         nation.n_name as n_name,
         postgres.part.p_partkey as p_partkey,
         postgres.part.p_mfgr as p_mfgr,
         postgres.supplier.s_address as s_address,
         postgres.supplier.s_phone as s_phone,
         postgres.supplier.s_comment as s_comment
     from
         postgres.part
             join postgres.partsupp on postgres.part.p_partkey = postgres.partsupp.ps_partkey
             join postgres.supplier on postgres.supplier.s_suppkey = postgres.partsupp.ps_suppkey
             join nation on postgres.supplier.s_nationkey = nation.n_nationkey
             join postgres.region on nation.n_regionkey = postgres.region.r_regionkey
             join tmpTable on tmpTable.p_key = postgres.part.p_partkey and postgres.partsupp.ps_supplycost = tmpTable.minCost
     where
             postgres.part.p_size = 15
       and postgres.region.r_name = 'EUROPE'
       and postgres.part.p_type like "^.*BRASS"
     order by
         nation.n_name,
         postgres.supplier.s_name,
         postgres.part.p_partkey
    )
order by s_acctbal desc;