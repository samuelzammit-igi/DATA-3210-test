

with

stg_product as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_product"
),

final as (
    select
        product_id,
        product_name,
        product_group_id
    from
        stg_product
)

select * from final