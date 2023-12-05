create view "dbt_szammit"."scd_type_of_event_type__dbt_tmp" as
    -- This file is automatically generated

with

type_of_event_type as (
    select * from "IGI_PROD_DW"."dbo"."TYPEOFEVENTTPE"
),

ordered as (
    select
        lTypeofEventKey,
        sTypeofEventCode,
        sTypeofEventDescr,
        sSearchTypeofEventDescr,
        dw_loadts,
        row_number() over (
            partition by
                lTypeofEventKey,
                sTypeofEventCode,
                sTypeofEventDescr,
                sSearchTypeofEventDescr
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_event_type
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lTypeofEventKey,
        sTypeofEventCode,
        sTypeofEventDescr,
        sSearchTypeofEventDescr,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lTypeofEventKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
