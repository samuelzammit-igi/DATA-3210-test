create view "dbt_dev"."dmn_policy__dbt_tmp" as
    

with

policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy" where _valid_to is null
),

master_declaration_attributes as (
    select * from "IGI_PROD_DW"."dbt_dev"."dmn_master_declaration_attributes"
),

attributed_policy as (
    select
        policy.*,
        m_d_attributes.declaration_policy_id,
        m_d_attributes.declaration_policy_activity_id,
        m_d_attributes.master_policy_id,
        m_d_attributes.master_policy_activity_id,
        m_d_attributes.master_uw_year,
        m_d_attributes.master_mis_uw_year
    from
        policy
        left join master_declaration_attributes as m_d_attributes on
            policy.lPolicyKey = m_d_attributes.policy_id
)

select * from attributed_policy
