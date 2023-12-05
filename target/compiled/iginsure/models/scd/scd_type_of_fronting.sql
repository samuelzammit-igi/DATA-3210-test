-- This file is automatically generated


with

type_of_fronting as (
    select * from "IGI_PROD_DW"."dbo"."TypeofFronting"
),

ordered as (
    select
        lFrontingKey,
        sFrontingCode,	
        sFrontingDescr,	
        sSearchFrontingDescr,	
        dw_loadts,
        row_number() over (
            partition by
                lFrontingKey,
                sFrontingCode,	
                sFrontingDescr,	
                sSearchFrontingDescr
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_fronting
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lFrontingKey,
        sFrontingCode,	
        sFrontingDescr,	
        sSearchFrontingDescr,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lFrontingKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged