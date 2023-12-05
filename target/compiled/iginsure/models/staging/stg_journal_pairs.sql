with

journal_pair as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_journal_pair" where _valid_to is null
),

entity_type as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_entity_type" where _valid_to is null
),

stg_account_period as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_account_period"
),

stg_division as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_division"
),

stg_subdivision as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_subdivision"
),

journal_pairs_flattened as (
    select
        journal_pair."lJournalPairKey" as journal_pair_id,
        journal_pair."lJournalActivityKey" as journal_activity_id,
        stg_account_period.period_year_period as account_period,
        journal_pair."dtROE" as roe_date,
        pair_entity_type."SENTITY" as journal_pair_entity_type,
        journal_pair."lInstanceKey" as journal_pair_instance_id,
        group_entity_type."SENTITY" as group_by_entity_type,
        journal_pair."sDescription" as description,
        stg_division.division,
        stg_subdivision.subdivision
    from
        journal_pair
        inner join entity_type as pair_entity_type on
            journal_pair."lEntityKey" = pair_entity_type."lEntityKey"
        inner join entity_type as group_entity_type on
            journal_pair."lGroupByEntityKey" = group_entity_type."lEntityKey"
        inner join stg_account_period on
            journal_pair."lAccountPeriodKey" = stg_account_period.account_period_id
        inner join stg_division on
            journal_pair."lDivisionKey" = stg_division.division_id
        inner join stg_subdivision on
            journal_pair."lSubdivisionKey" = stg_subdivision.subdivision_id
)

select * from journal_pairs_flattened