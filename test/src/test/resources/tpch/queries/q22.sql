SELECT
    cntrycode,
    COUNT( c1.c_acctbal ) AS numcust,
    SUM( c1.c_acctbal ) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING( c_phone, 1, 2 ) AS cntrycode,
            c_acctbal
        FROM
            customer AS c1
        WHERE
            (
                SUBSTRING( c1.c_phone, 1, 2 )= '13'
                OR SUBSTRING( c1.c_phone, 1, 2 )= '31'
                OR SUBSTRING( c1.c_phone, 1, 2 )= '23'
                OR SUBSTRING( c1.c_phone, 1, 2 )= '29'
                OR SUBSTRING( c1.c_phone, 1, 2 )= '30'
                OR SUBSTRING( c1.c_phone, 1, 2 )= '18'
                OR SUBSTRING( c1.c_phone, 1, 2 )= '17'
            )
            AND c1.c_acctbal >(
                SELECT
                    AVG( c_acctbal )
                FROM
                    customer AS c2
                WHERE
                    c2.c_acctbal > 0.00
                    AND(
                        SUBSTRING( c2.c_phone, 1, 2 )= '13'
                        OR SUBSTRING( c2.c_phone, 1, 2 )= '31'
                        OR SUBSTRING( c2.c_phone, 1, 2 )= '23'
                        OR SUBSTRING( c2.c_phone, 1, 2 )= '29'
                        OR SUBSTRING( c2.c_phone, 1, 2 )= '30'
                        OR SUBSTRING( c2.c_phone, 1, 2 )= '18'
                        OR SUBSTRING( c2.c_phone, 1, 2 )= '17'
                    )
            )
            AND NOT EXISTS(
                SELECT
                    o_custkey
                FROM
                    orders
                WHERE
                    orders.o_custkey = c1.c_custkey
            )
    )
GROUP BY
    cntrycode
ORDER BY
    cntrycode;