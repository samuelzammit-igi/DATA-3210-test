-- This file is automatically generated

with

journal_line_item as (
    select * from "IGI_PROD_DW"."dbo"."JournalLineItem"
),

ordered as (
    select
        lJournalLineItemKey,
        lJournalPairKey,
        lJournalActivityKey,
        lTypeOfDebitCreditKey,
        lAccountKey,
        lOriginalCurrencyKey,
        dOriginalROE,
        dOriginalAmount,
        lReportingCurrencyKey,
        dReportingROE,
        dReportingAmount,
        dw_loadts,
        row_number() over (
            partition by
                lJournalLineItemKey,
                lJournalPairKey,
                lJournalActivityKey,
                lTypeOfDebitCreditKey,
                lAccountKey,
                lOriginalCurrencyKey,
                dOriginalROE,
                dOriginalAmount,
                lReportingCurrencyKey,
                dReportingROE,
                dReportingAmount
            order by
                dw_loadts
        ) as dw_extract_order
    from
        journal_line_item
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lJournalLineItemKey,
        lJournalPairKey,
        lJournalActivityKey,
        lTypeOfDebitCreditKey,
        lAccountKey,
        lOriginalCurrencyKey,
        dOriginalROE,
        dOriginalAmount,
        lReportingCurrencyKey,
        dReportingROE,
        dReportingAmount,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lJournalLineItemKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged