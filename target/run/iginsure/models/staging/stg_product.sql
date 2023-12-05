create view "dbt_dev"."stg_product__dbt_tmp" as
    

with
product as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_product" where _valid_to is null
),

renamed as (
    select
        "LPRODUCTKEY" as product_id,
        "SPRODUCT" as product_name,
        "lTypeOfProductGroupKey" as product_group_id
    from
        product
)

select * from renamed
