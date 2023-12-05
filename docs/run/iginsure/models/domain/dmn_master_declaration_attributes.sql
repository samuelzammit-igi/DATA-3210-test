create view "dbt_dev"."dmn_master_declaration_attributes__dbt_tmp" as
    with
policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy" where _valid_to is null
),

policy_activity as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_activity" where _valid_to is null
),

policy_line as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_line" where _valid_to is null
),

type_of_placement as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_placement" where _valid_to is null
),

master_declaration_attributes as (
    select
        policy.lPolicyKey as policy_id,
        declaration_policy.lPolicyKey as declaration_policy_id,
        declaration_policy_activity.lPolicyActivityKey as declaration_policy_activity_id,
        master_policy.lPolicyKey as master_policy_id,
        master_policy_activity.lPolicyActivityKey as master_policy_activity_id,
        master_policy.nPeriodFromYear as master_uw_year,
        master_policy_line.dtUWyear as master_mis_uw_year
    from
        policy
        inner join policy as declaration_policy on
            policy.lMasterPolicyKey = declaration_policy.lPolicyKey
        inner join policy as master_policy on
            declaration_policy.lMasterPolicyKey = master_policy.lPolicyKey
        inner join policy_activity as declaration_policy_activity on
            declaration_policy.lActivePolicyActivityKey = declaration_policy_activity.lPolicyActivityKey
        inner join policy_line as declaration_policy_line on
            declaration_policy_activity.lPolicyActivityKey = declaration_policy_line.lPolicyActivityKey
        inner join policy_activity as master_policy_activity on
            master_policy.lActivePolicyActivityKey = master_policy_activity.lPolicyActivityKey
        inner join policy_line as master_policy_line on
            master_policy_activity.lPolicyActivityKey = master_policy_line.lPolicyActivityKey
        inner join type_of_placement on
            declaration_policy_activity.lTypeOfPlacementKey = type_of_placement.lTypeOfPlacementKey
    where
        type_of_placement.sMOP = 'Declaration'
        and declaration_policy.bIsSequence = 0
)

select * from master_declaration_attributes
