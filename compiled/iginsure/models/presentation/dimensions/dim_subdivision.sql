with subdivision_from_source AS (

    select distinct subdivision,subdivision_desc from 
    "IGI_PROD_DW"."dbt_dev"."stg_subdivision"
    where subdivision is not null

)

select * from subdivision_from_source