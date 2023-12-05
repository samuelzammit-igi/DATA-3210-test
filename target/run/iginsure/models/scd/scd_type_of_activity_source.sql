create view "dbt_dev"."scd_type_of_activity_source__dbt_tmp" as
    with

type_of_activity_source as (
    select * from "IGI_PROD_DW"."dbo"."typeofactivitysource"
),

ordered as (
    select
        lTypeofActivitySourceKey,
        sTypeofActivitySourceCode,
        sTypeofActivitySourceDescr,
        sSearchTypeofActivitySourceDescr,
        dw_loadts,
        row_number() over (
            partition by
                lTypeofActivitySourceKey,
                sTypeofActivitySourceCode,
                sTypeofActivitySourceDescr,
                sSearchTypeofActivitySourceDescr
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_activity_source
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lTypeofActivitySourceKey,
        sTypeofActivitySourceCode,
        sTypeofActivitySourceDescr,
        sSearchTypeofActivitySourceDescr,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lTypeofActivitySourceKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
