-- This file is automatically generated

with

journal_pair as (
    select * from "IGI_PROD_DW"."dbo"."JournalPair"
),

ordered as (
    select
        lJournalPairKey,
        lJournalActivityKey,
        lTemplateJournalEntryKey,
        lJournalKey,
        lAccrualStatusKey,
        lAccountPeriodKey,
        dtROE,
        lEntityKey,
        lInstanceKey,
        lGroupByEntityKey,
        lGroupByInstanceKey,
        sDescription,
        lDivisionKey,
        lSubDivisionKey,
        lTypeOfJournalExportGroupKey,
        dw_loadts,
        row_number() over (
            partition by
                lJournalPairKey,
                lJournalActivityKey,
                lTemplateJournalEntryKey,
                lJournalKey,
                lAccrualStatusKey,
                lAccountPeriodKey,
                dtROE,
                lEntityKey,
                lInstanceKey,
                lGroupByEntityKey,
                lGroupByInstanceKey,
                sDescription,
                lDivisionKey,
                lSubDivisionKey,
                lTypeOfJournalExportGroupKey
            order by
                dw_loadts
        ) as dw_extract_order
    from
        journal_pair
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lJournalPairKey,
        lJournalActivityKey,
        lTemplateJournalEntryKey,
        lJournalKey,
        lAccrualStatusKey,
        lAccountPeriodKey,
        dtROE,
        lEntityKey,
        lInstanceKey,
        lGroupByEntityKey,
        lGroupByInstanceKey,
        sDescription,
        lDivisionKey,
        lSubDivisionKey,
        lTypeOfJournalExportGroupKey,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lJournalPairKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged