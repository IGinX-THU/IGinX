SELECT
    cntrycode,
    COUNT( customer.c_acctbal ) AS numcust,
    SUM( customer.c_acctbal ) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING( c_phone, 1, 2 ) AS cntrycode,
            c_acctbal
        FROM
            customer
        WHERE
            (
                SUBSTRING( customer.c_phone, 1, 2 )= '13'
                OR SUBSTRING( customer.c_phone, 1, 2 )= '31'
                OR SUBSTRING( customer.c_phone, 1, 2 )= '23'
                OR SUBSTRING( customer.c_phone, 1, 2 )= '29'
                OR SUBSTRING( customer.c_phone, 1, 2 )= '30'
                OR SUBSTRING( customer.c_phone, 1, 2 )= '18'
                OR SUBSTRING( customer.c_phone, 1, 2 )= '17'
            )
            AND customer.c_acctbal >(
                SELECT
                    AVG( c_acctbal )
                FROM
                    customer
                WHERE
                    customer.c_acctbal > 0.00
                    AND(
                        SUBSTRING( customer.c_phone, 1, 2 )= '13'
                        OR SUBSTRING( customer.c_phone, 1, 2 )= '31'
                        OR SUBSTRING( customer.c_phone, 1, 2 )= '23'
                        OR SUBSTRING( customer.c_phone, 1, 2 )= '29'
                        OR SUBSTRING( customer.c_phone, 1, 2 )= '30'
                        OR SUBSTRING( customer.c_phone, 1, 2 )= '18'
                        OR SUBSTRING( customer.c_phone, 1, 2 )= '17'
                    )
            )
            AND NOT EXISTS(
                SELECT
                    o_custkey
                FROM
                    orders
                WHERE
                    orders.o_custkey = customer.c_custkey
            )
    )
GROUP BY
    cntrycode
ORDER BY
    cntrycode;