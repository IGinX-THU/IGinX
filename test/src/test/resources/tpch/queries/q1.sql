select
    lineitem.l_returnflag as l_returnflag,
    lineitem.l_linestatus as l_linestatus,
    sum(lineitem.l_quantity) as sum_qty,
    sum(lineitem.l_extendedprice) as sum_base_price,
    sum(tmp1) as sum_disc_price,
    sum(tmp2) as sum_charge,
    avg(lineitem.l_quantity) as avg_qty,
    avg(lineitem.l_extendedprice) as avg_price,
    avg(lineitem.l_discount) as avg_disc,
    count(lineitem.l_returnflag) as count_order
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
             lineitem
         where
                 lineitem.l_shipdate <= 904694400000
     )
group by
    lineitem.l_returnflag,
    lineitem.l_linestatus
order by
    lineitem.l_returnflag,
    lineitem.l_linestatus;