insert into tmpTable(key, p_partkey, val) values (
 select
     postgres.part.p_partkey,
     0.2 * tmp
 from (
  select
    postgres.part.p_partkey,
    avg(mongotpch.lineitem.l_quantity) as tmp
  from
    mongotpch.lineitem
    join postgres.part on mongotpch.lineitem.l_partkey = postgres.part.p_partkey
  group by postgres.part.p_partkey
  )
)

select
    tmp2 / 7 as avg_yearly
from (
     select
         sum(mongotpch.lineitem.l_extendedprice) as tmp2
     from
         mongotpch.lineitem
         join postgres.part on postgres.part.p_partkey = mongotpch.lineitem.l_partkey
         join tmpTable on tmpTable.p_partkey = mongotpch.lineitem.l_partkey
     where
       postgres.part.p_brand = 'Brand#23'
       and postgres.part.p_container = 'MED BOX'
       and mongotpch.lineitem.l_quantity < tmpTable.val
 );