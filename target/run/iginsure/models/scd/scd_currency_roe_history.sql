create view "dbt_szammit"."scd_currency_roe_history__dbt_tmp" as
    -- This file is automatically generated

with

currency_roe_history as (
    select * from "IGI_PROD_DW"."dbo"."currencyroehistory"
),

ordered as (
    select
        lCurrencyROEHistoryKey,
        lCurrencyKey,
        dtMonthEnd,
        dtActiveFrom,
        dtActiveTo,
        dROE,
        dw_loadts,
        row_number() over (
            partition by

                lCurrencyROEHistoryKey,
                lCurrencyKey,
                dtMonthEnd,
                dtActiveFrom,
                dtActiveTo,
                dROE,
                dw_loadts
            order by
                dw_loadts
        ) as dw_extract_order
    from
        currency_roe_history
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lCurrencyROEHistoryKey,
        lCurrencyKey,
        dtMonthEnd,
        dtActiveFrom,
        dtActiveTo,
        dROE,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lCurrencyROEHistoryKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
