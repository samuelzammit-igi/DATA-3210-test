with clm_audit_state_transitions as (
    select 
        linstancekey ,MIN(DTTRANSITION) DTTRANSITION

    from 
        "IGI_PROD_DW"."dbt_dev"."scd_audit_state_transitions" 
    where 
        "LENTITYKEY" = 531 --claim movement
        and _valid_to is null
        and lentitystatememberkey in (2262,2263) -- Authorised or paid
    group by linstancekey
)

select * from clm_audit_state_transitions