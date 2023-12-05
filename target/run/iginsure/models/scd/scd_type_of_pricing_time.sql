create view "dbt_dev"."scd_type_of_pricing_time__dbt_tmp" as
    with

type_of_pricing_time as (
    select * from "IGI_PROD_DW"."dbo"."TypeOfPricingTime"
),

ordered as (
    select
        lTypeOfPricingTimeKey,
        sCode,
        sPricingTime,
        sPricingTimeSearch,
        dw_loadts,
        row_number() over (
            partition by
           lTypeOfPricingTimeKey,
           sCode,
           sPricingTime,
           sPricingTimeSearch
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_pricing_time
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lTypeOfPricingTimeKey,
        sCode,
        sPricingTime,
        sPricingTimeSearch,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lTypeOfPricingTimeKey,sCode,sPricingTime,sPricingTimeSearch order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
