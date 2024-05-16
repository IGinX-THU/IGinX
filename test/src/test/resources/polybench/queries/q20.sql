insert into tmpTableA(key, partkey, suppkey, val) values (
                                                             select
                                                             partkey,
                                                             suppkey,
                                                             0.5 * tmp from(
      select
		      l_partkey as partkey,
		      l_suppkey as suppkey,
          sum(l_quantity) as tmp
      from
          mongotpch.lineitem
      where
          mongotpch.lineitem.l_shipdate >= 757353600000
          and mongotpch.lineitem.l_shipdate < 788889600000
      group by l_partkey, l_suppkey
  )
                                                         );

select
    postgres.supplier.s_name,
    postgres.supplier.s_address
from
    postgres.supplier
        join nation on postgres.supplier.s_nationkey = nation.n_nationkey
where
        postgres.supplier.s_suppkey in (
        select
            postgres.partsupp.ps_suppkey
        from
            postgres.partsupp
                join tmpTableA on tmpTableA.suppkey = postgres.partsupp.ps_suppkey and tmpTableA.partkey  = postgres.partsupp.ps_partkey
        where
                postgres.partsupp.ps_partkey in (
                select
                    p_partkey
                from
                    postgres.part
                where
                        postgres.part.p_name like 'forest.*'
            )
          and postgres.partsupp.ps_availqty > tmpTableA.val
    )
  and nation.n_name = 'CANADA'
order by
    postgres.supplier.s_name;