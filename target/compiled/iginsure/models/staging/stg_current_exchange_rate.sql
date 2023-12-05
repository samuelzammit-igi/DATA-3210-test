with currency_roe_history as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_currency_roe_history" where _valid_to is null
),

current_rates as (
    SELECT lCurrencyKey, dROE, dtActiveFrom 
    FROM currency_roe_history CH 
    WHERE (SELECT CAST(MAX(dtActiveFrom) AS DATE) FROM currency_roe_history ) BETWEEN dtActiveFrom AND dtActiveTo
)

select * from current_rates