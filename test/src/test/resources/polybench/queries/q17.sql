insert into tmpTableB(key, p_partkey, val) values (
 select
     tpchdata.part.p_partkey,
     0.2 * tmp
 from (
  select
    tpchdata.part.p_partkey,
    avg(mongotpch.lineitem.l_quantity) as tmp
  from
    mongotpch.lineitem
    join tpchdata.part on mongotpch.lineitem.l_partkey = tpchdata.part.p_partkey
  group by tpchdata.part.p_partkey
  )
);

select
    tmp2 / 7 as avg_yearly
from (
     select
         sum(mongotpch.lineitem.l_extendedprice) as tmp2
     from
         mongotpch.lineitem
         join tpchdata.part on tpchdata.part.p_partkey = mongotpch.lineitem.l_partkey
         join tmpTableB on tmpTableB.p_partkey = mongotpch.lineitem.l_partkey
     where
       tpchdata.part.p_brand = 'Brand#23'
       and tpchdata.part.p_container = 'MED BOX'
       and mongotpch.lineitem.l_quantity < tmpTableB.val
 );