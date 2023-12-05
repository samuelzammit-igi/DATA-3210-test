create view "dbt_szammit"."dmn_policy_account_periods__dbt_tmp" as
    with

dmn_policy_part as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_policy_part"
),

stg_account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_account_period"
),

max_activities as (
    select
        dmn_policy_part.policy_id,
        stg_account_period.period_year_period as account_period,
        stg_account_period.period_year as account_period_year,
        stg_account_period.period_month as account_period_month,
        max(dmn_policy_part.activity_id) as max_activity_id
    from
        dmn_policy_part
        cross join stg_account_period
    where
        dmn_policy_part.activity_state_is_active = 'TRUE'
        and dmn_policy_part.written_account_period <= stg_account_period.period_year_period
    group by
        policy_id,
        period_year_period,
        period_year,
        period_month
),

policy_account_periods as (
    select
        max_activities.policy_id,
        max_activities.account_period,
        max_activities.account_period_year,
        max_activities.account_period_month,
        datefromparts(max_activities.account_period_year, max_activities.account_period_month, 1) as month,
        max_activities.max_activity_id,
        dmn_policy_part.line_division as division,
        dmn_policy_part.line_subdivision as subdivision,
        dmn_policy_part.line_producing_office as producing_office,
        dmn_policy_part.line_product_name as product_name,
        dmn_policy_part.line_region as region,
        dmn_policy_part.line_producer as producer,
        dmn_policy_part.activity_state,
        dmn_policy_part.activity_state_is_active,
        dmn_policy_part.activity_period_from,
        dmn_policy_part.activity_period_to,
        dmn_policy_part.product_segregation_name,
        dmn_policy_part.policy_reference,
        dmn_policy_part.coverage_description,
        dmn_policy_part.coverage_code,
        dmn_policy_part.section_class


    from
        max_activities
        inner join dmn_policy_part on
            max_activities.max_activity_id = dmn_policy_part.activity_id
)

select * from policy_account_periods
