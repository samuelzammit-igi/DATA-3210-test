with producing_office_from_source as (


    select distinct line_producing_office as producing_office from 
    "IGI_PROD_DW"."dbt_dev"."dmn_policy_part"

    where line_producing_office is not null and line_producing_office <> 'Not Available'

)


select * from producing_office_from_source