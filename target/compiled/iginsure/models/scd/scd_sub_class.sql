-- This file is automatically generated


with

sub_class as (
    select * from "IGI_PROD_DW"."dbo"."SubClass"
),

ordered as (
    select
        lSubClassKey,
        sSubClassCode,
        sSubClassDescr,
        sSearchSubClassDescr,
        lClassKey,
        dw_loadts,
        row_number() over (
            partition by
                lSubClassKey,
                sSubClassCode,
                sSubClassDescr,
                sSearchSubClassDescr,
                lClassKey
            order by
                dw_loadts
        ) as dw_extract_order
    from
        sub_class
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lSubClassKey,
        sSubClassCode,
        sSubClassDescr,
        sSearchSubClassDescr,
        lClassKey,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lSubClassKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged