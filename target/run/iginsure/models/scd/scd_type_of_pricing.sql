create view "dbt_dev"."scd_type_of_pricing__dbt_tmp" as
    with

type_of_pricing as (
    select * from "IGI_PROD_DW"."dbo"."TypeOfPricing"
),

ordered as (
    select
       lTypeOfPricingKey,
       sCode,
       sPricingType,
       sPricingTypeSearch,
       dw_loadts,
        row_number() over (
            partition by
            lTypeOfPricingKey,
            sCode,
            sPricingType,
            sPricingTypeSearch
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_pricing
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
       lTypeOfPricingKey,
       sCode,
       sPricingType,
       sPricingTypeSearch,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lTypeOfPricingKey,sCode,sPricingType,sPricingTypeSearch order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
