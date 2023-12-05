create view "dbt_szammit"."stg_latest_activity_per_renewal_policy__dbt_tmp" as
    with policy_line as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_policy_line" where _valid_to is null
),

policy_activity as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_policy_activity" where _valid_to is null
),

latest_activivty_id as (

    select pa.lpolicykey,
           MAX(pa.lPolicyActivityKey) as latest_activity_id
    from policy_activity pa 
    inner join policy_line pl on pa.lPolicyActivityKey = pl.lPolicyActivityKey
    where pl.llinkrenewalpolicykey is not null
    group by pa.lpolicykey

)


select * from latest_activivty_id
