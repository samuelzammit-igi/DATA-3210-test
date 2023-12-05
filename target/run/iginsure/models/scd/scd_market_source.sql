create view "dbt_szammit"."scd_market_source__dbt_tmp" as
    -- This file is automatically generated



with

market_source as (
    select * from "IGI_PROD_DW"."dbo"."MarketSource"
),

ordered as (
    select
        lMarketSourceKey,
        sCode,
        sMarketSource,
        sMarketSourceSearch,
        lTypeOfMarketSourceKey,
        dw_loadts,
        row_number() over (
            partition by
                lMarketSourceKey,
                sCode,
                sMarketSource,
                sMarketSourceSearch,
                lTypeOfMarketSourceKey
            order by
                dw_loadts
        ) as dw_extract_order
    from
        market_source
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lMarketSourceKey,
        sCode,
        sMarketSource,
        sMarketSourceSearch,
        lTypeOfMarketSourceKey,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lMarketSourceKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
