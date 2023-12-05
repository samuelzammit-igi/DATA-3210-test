


with account_period as (


    select * from "IGI_PROD_DW"."dbt_dev"."stg_account_period" 

),


audit_state_transitions_full as (
    select 
        *
     from 
         "IGI_PROD_DW"."dbt_dev"."scd_audit_state_transitions" 
        
    where LENTITYKEY = 429 
),

account_periods as (

    select distinct ap.period_year_period as account_period,ast.DTTRANSITION as date_closed
    from account_period ap
    inner join audit_state_transitions_full ast on ast.LINSTANCEKEY = ap.account_period_id and ast.LENTITYKEY = 429 
    where ast.LENTITYSTATEMEMBERKEY = 2087 --closed

)


select * from account_periods