select sum(tmp) from (
     select
             l_extendedprice * l_discount as tmp
     from
         mongotpch.lineitem
     where
             mongotpch.lineitem.l_shipdate >= 757353600000
       and mongotpch.lineitem.l_shipdate < 788889600000 + 28800000
       and mongotpch.lineitem.l_discount >= 0.05
       and mongotpch.lineitem.l_discount <= 0.07
       and mongotpch.lineitem.l_quantity < 24
 );
