SELECT
    MAX(
        CASE 
            WHEN INSTR(CAST(ABS(amount) AS STRING), '.') > 0 THEN
                LENGTH(CAST(ABS(amount) AS STRING)) - 1  -- Subtract 1 to exclude the decimal point
            ELSE
                LENGTH(CAST(ABS(amount) AS STRING))
        END
    ) AS max_precision,
    MAX(
        CASE 
            WHEN INSTR(CAST(ABS(amount) AS STRING), '.') > 0 THEN
                LENGTH(SUBSTR(CAST(ABS(amount) AS STRING), INSTR(CAST(ABS(amount) AS STRING), '.') + 1))
            ELSE
                0
        END
    ) AS max_scale,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT amount) AS distinct_amounts
FROM your_database.sales
WHERE amount IS NOT NULL;

