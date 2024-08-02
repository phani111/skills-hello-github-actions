SELECT
    MAX(
        CASE 
            WHEN INSTR(CAST(ABS(amount) AS STRING), '.') > 0 THEN
                LENGTH(CAST(ABS(amount) AS STRING)) - INSTR(CAST(ABS(amount) AS STRING), '.')
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
    ) AS max_scale
FROM your_database.sales
WHERE amount IS NOT NULL;
