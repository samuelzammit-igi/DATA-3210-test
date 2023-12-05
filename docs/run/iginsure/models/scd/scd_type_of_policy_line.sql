create view "dbt_dev"."scd_type_of_policy_line__dbt_tmp" as
    with type_of_policy_line as (

    SELECT * FROM  "IGI_PROD_DW"."dbo"."TYPEOFPOLICYLINE"

),


ordered as (

    select 

    lTypeofPolicyLineKey,
    sTypeofPolicyLineCode,
    sTypeofPolicyLineDescr,
    sSearchTypeofPolicyLineDescr,
    dw_loadts,
    row_number()OVER(
        partition by
        lTypeofPolicyLineKey,
        sTypeofPolicyLineCode,
        sTypeofPolicyLineDescr,
        sSearchTypeofPolicyLineDescr
        order by
        dw_loadts) AS  dw_extract_order

    from type_of_policy_line


    
    ),

filtered as (
    select * from ordered where dw_extract_order = 1
),


ranged as (
    select
        lTypeofPolicyLineKey,
        sTypeofPolicyLineCode,
        sTypeofPolicyLineDescr,
        sSearchTypeofPolicyLineDescr,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lTypeofPolicyLineKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
