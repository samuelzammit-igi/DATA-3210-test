create view "dbt_szammit"."stg_journal_activities__dbt_tmp" as
    with

journal_activity as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_journal_activity" where _valid_to is null
),

entity_type as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_entity_type" where _valid_to is null
),

journal_template as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_journal_template" where _valid_to is null
),

journal_pairs_flattened as (
    select
        journal_activity."lJournalActivityKey" as journal_activity_id,
        journal_activity."dtEntryDate" as journal_entry_date,
        source_entity_type."SENTITY" as journal_source_entity_type,
        journal_activity."lSourceInstanceKey" as journal_source_instance_id,
        journal_template."lTemplateJournalActivityKey" as journal_template_id,
        journal_template."sTemplateDescription" as journal_template_description
    from
        journal_activity
        inner join entity_type as source_entity_type on
            journal_activity."lSourceEntityKey" = source_entity_type."lEntityKey"
        inner join journal_template on
            journal_activity."lTemplateJournalActivityKey" = journal_template."lTemplateJournalActivityKey"
)

select * from journal_pairs_flattened
