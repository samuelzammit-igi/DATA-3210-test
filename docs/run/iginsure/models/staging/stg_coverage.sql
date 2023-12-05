create view "dbt_dev"."stg_coverage__dbt_tmp" as
    

with
policy_liability as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_liability" where _valid_to is null
),

type_of_sub_class as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_sub_class" where _valid_to is null
),

liability_sub_class as (
    select
        min("lPolicyLiabilityKey") as policy_liability_id,
        "lPolicyKey" as policy_id,
        "lPolicySectionKey" as policy_section_id,
        "lPolicyActivityKey" as policy_activity_id,
        type_of_sub_class."SSUBCLASS" as coverage_description,
        type_of_sub_class."sCode" as coverage_code
    from
        policy_liability
        inner join type_of_sub_class on
            policy_liability."lTypeOfSubClassKey" = type_of_sub_class."LTYPEOFSUBCLASSKEY"
    group by
        lPolicyKey, lPolicySectionKey, lPolicyActivityKey, SSUBCLASS, sCode
),

ordering as (
    select *,
        row_number() over (partition by policy_section_id order by policy_liability_id) as coverage_number
    from
        liability_sub_class
),

coverage as (
    select
        first.policy_id,
        first.policy_section_id,
        first.coverage_description as first_coverage_description,
        first.coverage_code as first_coverage_code,
        second.coverage_description as second_coverage_description,
        second.coverage_code as second_coverage_code
    from
        ordering as first
        left join ordering as second on
            first.policy_section_id = second.policy_section_id
            and first.policy_liability_id < second.policy_liability_id
    where
        first.coverage_number = 1
        and (second.coverage_number is null or second.coverage_number = 2)
),

descriptions as (
    select
        *,
        first_coverage_description + case when second_coverage_description is not null then ', ' + second_coverage_description else '' end as coverage_description,
        first_coverage_code + case when second_coverage_code is not null then ', ' + second_coverage_code else '' end as coverage_code
    from
        coverage
)

select * from descriptions
