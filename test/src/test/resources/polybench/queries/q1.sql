select
    mongotpch.lineitem.l_returnflag as l_returnflag,
    mongotpch.lineitem.l_linestatus as l_linestatus,
    sum(mongotpch.lineitem.l_quantity) as sum_qty,
    sum(mongotpch.lineitem.l_extendedprice) as sum_base_price,
    sum(tmp1) as sum_disc_price,
    sum(tmp2) as sum_charge,
    avg(mongotpch.lineitem.l_quantity) as avg_qty,
    avg(mongotpch.lineitem.l_extendedprice) as avg_price,
    avg(mongotpch.lineitem.l_discount) as avg_disc,
    count(mongotpch.lineitem.l_returnflag) as count_order
from (
         select
             l_returnflag,
             l_linestatus,
             l_quantity,
             l_extendedprice,
             l_discount,
             l_extendedprice * (1 - l_discount) as tmp1,
             l_extendedprice * (1 - l_discount) * (1 + l_tax) as tmp2
         from
             mongotpch.lineitem
         where
                 mongotpch.lineitem.l_shipdate <= 904665600000 + 28800000
     )
group by
    mongotpch.lineitem.l_returnflag,
    mongotpch.lineitem.l_linestatus
order by
    mongotpch.lineitem.l_returnflag,
    mongotpch.lineitem.l_linestatus;