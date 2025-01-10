SELECT
    b.O_ORDERKEY AS ORDER_ID,
    b.O_CUSTKEY AS CUSTOMER_ID,
    b.O_ORDERDATE AS ORDER_DATE,
    (b.O_ORDERDATE + 20 * INTERVAL '1 day') AS TRANSACTION_DATE,
    (RPAD(CONCAT(b.O_ORDERKEY, b.O_CUSTKEY, TO_CHAR(b.O_ORDERDATE, 'YYYYMMDD')),  24, '0'))::NUMERIC AS TRANSACTION_NUMBER,
    b.O_TOTALPRICE AS AMOUNT,
    CAST(
    CASE ((ABS(RANDOM()::INT % 2)) + 1)
        WHEN 1 THEN 'DR'
        WHEN 2 THEN 'CR'
    END AS VARCHAR(2)) AS TYPE
FROM {{ source('tpch_sample', 'orders') }}  AS b
LEFT JOIN {{ source('tpch_sample', 'customer') }} AS c
    ON b.O_CUSTKEY = c.C_CUSTKEY
WHERE b.O_ORDERDATE = '{{ var('load_date') }}'::DATE