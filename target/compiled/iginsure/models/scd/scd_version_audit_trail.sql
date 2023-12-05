-- This file is automatically generated


with

version_audit_trail as (
    select * from "IGI_PROD_DW"."dbo"."versionaudittrail"
),

ordered as (
    select
        LVERSIONAUDITTRAILKEY,
        LENTITYKEY,
        LENTITYINSTANCEKEY,
        DTFROM,
        DTTO,
        LPREVIOUSENTITYINSTANCEKEY,
        dw_loadts,
        row_number() over (
            partition by
                LVERSIONAUDITTRAILKEY,
                LENTITYKEY,
                LENTITYINSTANCEKEY,
                DTFROM,
                DTTO,
                LPREVIOUSENTITYINSTANCEKEY
            order by
                dw_loadts
        ) as dw_extract_order
    from
        version_audit_trail
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        LVERSIONAUDITTRAILKEY,
        LENTITYKEY,
        LENTITYINSTANCEKEY,
        DTFROM,
        DTTO,
        LPREVIOUSENTITYINSTANCEKEY,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by LVERSIONAUDITTRAILKEY order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged