SELECT
    SUM( revenue )
FROM
    (
        SELECT
            l_extendedprice * l_discount AS revenue
        FROM
            lineitem
        WHERE
            lineitem.l_shipdate >= 757353600000
            AND lineitem.l_shipdate < 788889600000
            AND lineitem.l_discount >= 0.05
            AND lineitem.l_discount <= 0.07
            AND lineitem.l_quantity < 24
    );