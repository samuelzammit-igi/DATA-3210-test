create view "dbt_szammit"."dmn_journal_pairs__dbt_tmp" as
    with

stg_journal_line_items as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_journal_line_items"
),

stg_journal_pairs as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_journal_pairs"
),

stg_journal_activities as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_journal_activities"
),

paired_line_items as (
    select
        credit.journal_pair_id,
        credit.original_currency,
        credit.original_roe,
        abs(credit.original_amount) as original_amount,
        credit.reporting_currency,
        abs(credit.reporting_amount) as reporting_amount,
        credit.account_number as credit_account_number,
        credit.account_type as credit_account_type,
        credit.account_description as credit_account_description,
        debit.account_number as debit_account_number,
        debit.account_type as debit_account_type,
        debit.account_description as debit_account_description

    from
        stg_journal_line_items as credit
        inner join stg_journal_line_items as debit on
            credit.journal_pair_id = debit.journal_pair_id
            and credit.debit_credit = 'Credit'
            and debit.debit_credit = 'Debit'
),

journal_pairs as (
    select
        stg_journal_pairs.journal_pair_id,
        stg_journal_pairs.journal_activity_id,
        stg_journal_activities.journal_entry_date,
        stg_journal_activities.journal_source_entity_type,
        stg_journal_activities.journal_source_instance_id,
        stg_journal_activities.journal_template_description,
        stg_journal_pairs.account_period,
        stg_journal_pairs.roe_date,
        stg_journal_pairs.journal_pair_entity_type,
        stg_journal_pairs.journal_pair_instance_id,
        stg_journal_pairs.group_by_entity_type,
        stg_journal_pairs.description,
        stg_journal_pairs.division,
        stg_journal_pairs.subdivision,
        paired_line_items.original_currency,
        paired_line_items.original_roe,
        paired_line_items.original_amount,
        paired_line_items.reporting_currency,
        paired_line_items.reporting_amount,
        paired_line_items.credit_account_number,
        paired_line_items.credit_account_type,
        paired_line_items.credit_account_description,
        paired_line_items.debit_account_number,
        paired_line_items.debit_account_type,
        paired_line_items.debit_account_description

    from
        stg_journal_pairs
        inner join paired_line_items on
            stg_journal_pairs.journal_pair_id = paired_line_items.journal_pair_id
        inner join stg_journal_activities on
            stg_journal_pairs.journal_activity_id = stg_journal_activities.journal_activity_id
)

select * from journal_pairs
