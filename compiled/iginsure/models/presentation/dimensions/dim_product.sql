with product_from_source as (


select distinct product_name
from "IGI_PROD_DW"."dbt_dev"."stg_product"

where product_name is not null

)

select * from product_from_source