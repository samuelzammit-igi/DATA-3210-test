-- This file is automatically generated


with

ZONE_MAJOR_TERRITORY as (
    select * from "IGI_PROD_DW"."dbo"."ZONEMAJORTERRITORY"
),

ordered as (
    select
        LMAJORTERRITORYKEY,
        SMAJORTERRITORYCODE,
        SMAJORTERRITORY,
        SMAJORTERRITORYSEARCH,
        dw_loadts,
        row_number() over (
            partition by
                LMAJORTERRITORYKEY,
                SMAJORTERRITORYCODE,
                SMAJORTERRITORY,
                SMAJORTERRITORYSEARCH,
                dw_loadts
                
            order by
                dw_loadts
        ) as dw_extract_order
    from
        ZONE_MAJOR_TERRITORY
),

filtered as (
    select * from ordered where dw_extract_order = 1
),


ranged as (
    select
        LMAJORTERRITORYKEY,
        SMAJORTERRITORYCODE,
        SMAJORTERRITORY,
        SMAJORTERRITORYSEARCH,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by LMAJORTERRITORYKEY order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged