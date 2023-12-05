-- This file is automatically generated

with

account as (
    select * from "IGI_PROD_DW"."dbo"."Account"
),

ordered as (
    select
        lAccountKey,
        sAccountNumber,
        sAccountDescription,
        sAccountDescriptionSearch,
        lTypeOfAccountElementKey,
        sExtractCode,
        bRevalueToBase,
        lExtractTypeOfAccountElementKey,
        bExtractNatureMismatch,
        dw_loadts,
        row_number() over (
            partition by
                lAccountKey,
                sAccountNumber,
                sAccountDescription,
                sAccountDescriptionSearch,
                lTypeOfAccountElementKey,
                sExtractCode,
                bRevalueToBase,
                lExtractTypeOfAccountElementKey,
                bExtractNatureMismatch
            order by
                dw_loadts
        ) as dw_extract_order
    from
        account
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lAccountKey,
        sAccountNumber,
        sAccountDescription,
        sAccountDescriptionSearch,
        lTypeOfAccountElementKey,
        sExtractCode,
        bRevalueToBase,
        lExtractTypeOfAccountElementKey,
        bExtractNatureMismatch,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lAccountKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged