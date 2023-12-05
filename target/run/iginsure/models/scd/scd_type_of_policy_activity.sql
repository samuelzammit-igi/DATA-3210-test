create view "dbt_dev"."scd_type_of_policy_activity__dbt_tmp" as
    with

type_of_policy_activity as (
    select * from "IGI_PROD_DW"."dbo"."typeofpolicyactivity"
),

ordered as (
    select
        lTypeOfPolicyActivityKey,
        sActivity,
        sActivitySearch,
        lWriteTypeOfAccountActivityKey,
        lSignTypeOfAccountActivityKey,
        sCode,
        sCode2,
        lTypeofCodeSetKey,
        dw_loadts,
        row_number() over (
            partition by
                lTypeOfPolicyActivityKey,
                sActivity,
                sActivitySearch,
                lWriteTypeOfAccountActivityKey,
                lSignTypeOfAccountActivityKey,
                sCode,
                sCode2,
                lTypeofCodeSetKey
            order by
                dw_loadts
        ) as dw_extract_order
    from
        type_of_policy_activity
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lTypeOfPolicyActivityKey,
        sActivity,
        sActivitySearch,
        lWriteTypeOfAccountActivityKey,
        lSignTypeOfAccountActivityKey,
        sCode,
        sCode2,
        lTypeofCodeSetKey,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lTypeOfPolicyActivityKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
