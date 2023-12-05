-- This file is automatically generated


with

type_of_class as (
    select * from "IGI_PROD_DW"."dbo"."TYPEOFCLASS"
),

ordered as (
    select
        LTYPEOFCLASSKEY,
        SCODE,
        SCLASS,
        SCLASSSEARCH,
        dw_loadts,
        row_number() over (
            partition by
                LTYPEOFCLASSKEY,
                SCODE,
                SCLASS,
                SCLASSSEARCH
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_class
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        LTYPEOFCLASSKEY,
        SCODE,
        SCLASS,
        SCLASSSEARCH,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by LTYPEOFCLASSKEY order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged