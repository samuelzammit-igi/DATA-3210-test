with region_from_policy as (

SELECT distinct territory as region
FROM
"IGI_PROD_DW"."dbt_dev"."stg_country_territory"
where territory is not null

)

select * from region_from_policy