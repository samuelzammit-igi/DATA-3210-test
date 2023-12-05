create view "dbt_szammit"."stg_journal_line_items__dbt_tmp" as
    with

line_items as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_journal_line_item" where _valid_to is null
),

currency as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_currency" where _valid_to is null
),

type_of_debit_credit as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_type_of_debit_credit" where _valid_to is null
),

type_of_accounting_element as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_type_of_accounting_element" where _valid_to is null
),

account as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_account" where _valid_to is null
),

line_items_flattened as (
    select
        line_items."lJournalLineItemKey" as journal_line_item_id,
        line_items."lJournalPairKey" as journal_pair_id,
        line_items."lJournalActivityKey" as journal_activity_id,
        type_of_debit_credit."sDebitCredit" as debit_credit,
        account."sAccountNumber" as account_number,
        type_of_accounting_element."sTypeOfAccountingElement" as account_type,
        account."sAccountDescription" as account_description,
        original_currency."sCcy" as original_currency,
        line_items."dOriginalROE" as original_roe,
        line_items."dOriginalAmount" as original_amount,
        reporting_currency."sCcy" as reporting_currency,
        line_items."dReportingAmount" as reporting_amount
    from
        line_items
        inner join type_of_debit_credit on
            line_items."lTypeOfDebitCreditKey" = type_of_debit_credit."lTypeOfDebitCreditKey"
        inner join account on
            line_items."lAccountKey" = account."lAccountKey"
        inner join type_of_accounting_element on
            account."lTypeOfAccountElementKey" = type_of_accounting_element."lTypeOfAccountingElementKey"
        inner join currency as original_currency on
            line_items."lOriginalCurrencyKey" = original_currency."lCurrencyKey"
        inner join currency as reporting_currency on
            line_items."lReportingCurrencyKey" = reporting_currency."lCurrencyKey"
)

select * from line_items_flattened
