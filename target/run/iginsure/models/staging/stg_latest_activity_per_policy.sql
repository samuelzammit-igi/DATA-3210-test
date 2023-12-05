create view "dbt_szammit"."stg_latest_activity_per_policy__dbt_tmp" as
    with policy_activity as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_policy_activity" where _valid_to is null
),


latest_activivty_id as (

    select lpolicykey,
           MAX(lPolicyActivityKey) as latest_activity_id
    from policy_activity
    group by lpolicykey

)


select * from latest_activivty_id
