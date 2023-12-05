create view "dbt_dev"."stg_written_account_period__dbt_tmp" as
    with audit_state_transitions_full as (
    select 
        *
     from 
        "IGI_PROD_DW"."dbt_dev"."stg_activity_audit_state_transitions" 
),

audit_state_transitions as (
    select
        LINSTANCEKEY,
        CAST(min(DTTRANSITION) AS DATE) as DTTRANSITION,
        min(DTTRANSITION)  as DTTRANSITION_for_account_period

    from
        audit_state_transitions_full
    where
        (
            "LENTITYSTATEMEMBERKEY" in (
                2153, -- 'Written'
                2220, -- 'Cancellation'
                2204, -- 'Cancelled'
                2533, -- 'Cancelled Adjustment'
                2162, -- 'Cancelled Endorsement'
                2392, -- 'Cancelled Contra'
                2391  -- 'Contra Applied'
            )
            or CAST("DTTRANSITION" AS DATE)  = '2019-03-16' -- Migrated
        )
    group by
        LINSTANCEKEY
),

version_audit_trail as (
    select
        LENTITYINSTANCEKEY,
        LPREVIOUSENTITYINSTANCEKEY
    from
        "IGI_PROD_DW"."dbt_dev"."scd_version_audit_trail"
    where
        LENTITYKEY = 611 -- Policy Activity
        and _valid_to is null
),


activity_effective_written_timestamp as (
    select
        version_audit_trail.LENTITYINSTANCEKEY as activity_id,
        version_audit_trail.LPREVIOUSENTITYINSTANCEKEY as previous_activity_id,
        coalesce (
            audit_state_transitions.DTTRANSITION,
            previous_audit_state_transitions.DTTRANSITION
        ) as effective_written_timestamp,
        coalesce (
            audit_state_transitions.DTTRANSITION_for_account_period,
            previous_audit_state_transitions.DTTRANSITION_for_account_period
        ) as effective_written_timestamp_for_ap

    from
        version_audit_trail
        left join audit_state_transitions on
            version_audit_trail.LENTITYINSTANCEKEY = audit_state_transitions.LINSTANCEKEY
        left join audit_state_transitions as previous_audit_state_transitions on
            version_audit_trail.LPREVIOUSENTITYINSTANCEKEY = previous_audit_state_transitions.LINSTANCEKEY
),

account_period_closing as (

    select * from "IGI_PROD_DW"."dbt_dev"."stg_account_periods_closing"

),


correct_written_account_period as (

select * from     
    (select *,ROW_NUMBER() over (partition by activity_ts.activity_id order by ap.date_closed) as ranked
    from 
    activity_effective_written_timestamp activity_ts
    inner join account_period_closing ap on 
    activity_ts.effective_written_timestamp_for_ap  <= ap.date_closed)final
where ranked = 1

)


select  * from correct_written_account_period
