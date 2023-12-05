

with

type_of_profit_centre as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_profit_centre" where _valid_to is null
),

topc as (
    select
        lTypeOfProfitCentreKey AS type_of_profit_centre_id,
        sPCName as producing_office
    from
        type_of_profit_centre
)

select * from topc