with iw_ow_flags as (


    select distinct iw_ow_flag from "IGI_PROD_DW"."dbt_dev"."premium_data"
    where iw_ow_flag is not null


)

select * from iw_ow_flags