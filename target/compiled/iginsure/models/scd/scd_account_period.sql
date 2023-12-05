-- This file is automatically generated


with

account_period as (
    select * from "IGI_PROD_DW"."dbo"."AccountPeriod"
),

ordered as (
    select
        lAccountPeriodKey,
        nYearPeriod,
        nYear,
        dtPeriodStart,
        dtPeriodEnd,
        dtEarnAt,
        nPeriod,
        sPeriodName,
        sPeriodNameSearch,
        bClosing,
        lPreviousAccountPeriodKey,
        nQuarterlyPeriod,
        lTypeOfAccountPeriodROEKey,
        sPeriodYearCode,
        dtPeriodFromSales,
        dtPeriodToSales,
        dw_loadts,
        row_number() over (
            partition by
                lAccountPeriodKey,
                nYearPeriod,
                nYear,
                dtPeriodStart,
                dtPeriodEnd,
                dtEarnAt,
                nPeriod,
                sPeriodName,
                sPeriodNameSearch,
                bClosing,
                lPreviousAccountPeriodKey,
                nQuarterlyPeriod,
                lTypeOfAccountPeriodROEKey,
                sPeriodYearCode,
                dtPeriodFromSales,
                dtPeriodToSales,
                dw_loadts
            order by
                dw_loadts
        ) as dw_extract_order
    from
        account_period
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lAccountPeriodKey,
        nYearPeriod,
        nYear,
        dtPeriodStart,
        dtPeriodEnd,
        dtEarnAt,
        nPeriod,
        sPeriodName,
        sPeriodNameSearch,
        bClosing,
        lPreviousAccountPeriodKey,
        nQuarterlyPeriod,
        lTypeOfAccountPeriodROEKey,
        sPeriodYearCode,
        dtPeriodFromSales,
        dtPeriodToSales,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lAccountPeriodKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged