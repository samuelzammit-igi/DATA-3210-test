

SELECT * FROM INFORMATION_SCHEMA.columns
WHERE TABLE_SCHEMA = (
    -- dbt gives us the full model name (db.schema.table)
    -- but we want the schema name only
    SELECT 
        REPLACE(value, '"', '')
    from STRING_SPLIT('"IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_clean_nb100"','.',1)
    where ordinal = 2
)
AND TABLE_NAME = (
    -- same as above, but we want the table name
    SELECT 
        REPLACE(value, '"', '')
    from STRING_SPLIT('"IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_clean_nb100"','.',1)
    where ordinal = 3
)
AND COLUMN_NAME = 'policy_inception_date_dev_dur_q'
AND DATA_TYPE != 'int'

