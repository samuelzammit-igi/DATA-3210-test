with activity_audit_state_transitions as (
    select 
        *

    from 
        "IGI_PROD_DW"."dbt_dev"."scd_audit_state_transitions" 
    where 
        "LENTITYKEY" = 611 -- 'Policy Activity'
        and _valid_to is null
)

select * from activity_audit_state_transitions