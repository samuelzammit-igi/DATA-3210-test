

with

account_period as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_account_period" where _valid_to is null
),

account_period_ranges as (
    select
        "lAccountPeriodKey" as account_period_id,
        "sPeriodName" as period_name,
        "dtPeriodStart" as period_start,
        "dtPeriodEnd" as period_end,
        "nYearPeriod" as period_year_period,
        "nYear" as period_year,
        "nYearPeriod"-("nYear"*100) as period_month
    from
        account_period
)

select * from account_period_ranges