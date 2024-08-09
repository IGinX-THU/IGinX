select sum(revenue)
from (
    select l_extendedprice * l_discount as revenue
    from lineitem
    where
        lineitem.l_shipdate >= 757353600000
        and lineitem.l_shipdate < 788889600000
        and lineitem.l_discount >= 0.05
        and lineitem.l_discount <= 0.07
        and lineitem.l_quantity < 24
);