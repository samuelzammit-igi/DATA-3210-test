-- This file is automatically generated


with

entity_matching_journal as (
    select * from "IGI_PROD_DW"."dbo"."EntityMatchingJournal"
),

ordered as (
    select
        lEntityMatchingJournalKey,
        lTasksEntityMatchingKey,
        lLeftEntityKey,
        lLeftEntityInstanceKey,
        lRightEntityKey,
        lRightEntityInstanceKey,
        dtEffective,
        dLeftAmount,
        dRightAmount,
        dLeftUnmatchedBalance,
        dRightUnmatchedBalance,
        lLeftCcyKey,
        lRightCcyKey,
        dLeftROEToBase,
        dRightROEToBase,
        lSecurityUserKey,
        dtConfirmed,
        lAccountPeriodKey,
        dRightToLeftROE,
        lTypeOfMatchHistoryKey,
        lUnmatchedJournalKey,
        bJournalProcessed,
        dw_loadts,
        row_number() over (
            partition by
                lEntityMatchingJournalKey,
                lTasksEntityMatchingKey,
                lLeftEntityKey,
                lLeftEntityInstanceKey,
                lRightEntityKey,
                lRightEntityInstanceKey,
                dtEffective,
                dLeftAmount,
                dRightAmount,
                dLeftUnmatchedBalance,
                dRightUnmatchedBalance,
                lLeftCcyKey,
                lRightCcyKey,
                dLeftROEToBase,
                dRightROEToBase,
                lSecurityUserKey,
                dtConfirmed,
                lAccountPeriodKey,
                dRightToLeftROE,
                lTypeOfMatchHistoryKey,
                lUnmatchedJournalKey,
                bJournalProcessed
            order by
                dw_loadts
        ) as dw_extract_order
    from
        entity_matching_journal
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lEntityMatchingJournalKey,
        lTasksEntityMatchingKey,
        lLeftEntityKey,
        lLeftEntityInstanceKey,
        lRightEntityKey,
        lRightEntityInstanceKey,
        dtEffective,
        dLeftAmount,
        dRightAmount,
        dLeftUnmatchedBalance,
        dRightUnmatchedBalance,
        lLeftCcyKey,
        lRightCcyKey,
        dLeftROEToBase,
        dRightROEToBase,
        lSecurityUserKey,
        dtConfirmed,
        lAccountPeriodKey,
        dRightToLeftROE,
        lTypeOfMatchHistoryKey,
        lUnmatchedJournalKey,
        bJournalProcessed,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lEntityMatchingJournalKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged