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
    tpchdata.supplier.s_name,
    tpchdata.supplier.s_address
from
    tpchdata.supplier
        join nation on tpchdata.supplier.s_nationkey = nation.n_nationkey
where
        tpchdata.supplier.s_suppkey in (
        select
            tpchdata.partsupp.ps_suppkey
        from
            tpchdata.partsupp
                join tmpTableA on tmpTableA.suppkey = tpchdata.partsupp.ps_suppkey and tmpTableA.partkey = tpchdata.partsupp.ps_partkey
        where
                tpchdata.partsupp.ps_partkey in (
                select
                    p_partkey
                from
                    tpchdata.part
                where
                        tpchdata.part.p_name like 'forest.*'
            )
          and tpchdata.partsupp.ps_availqty > tmpTableA.val
    )
  and nation.n_name = 'CANADA'
order by
    tpchdata.supplier.s_name;