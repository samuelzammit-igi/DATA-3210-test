create view "dbt_dev"."stg_contra_expiry_history__dbt_tmp" as
    with policyactivity as (

    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_activity" where _valid_to is null

),

Auditheader as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_auditheader" where _valid_to is null
),


Auditrows as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_auditrows" where _valid_to is null
),

Auditcolumns as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_auditcolumns" where _valid_to is null
),


contra_hist as (

    select * from 
        (select pa.lpolicyKey,
                pa.lpolicyactivitykey 'lpolicyactivitykey',
                DTDATEAMENDED, DTOLDVALUE 'Expirydate',
                LAUDITCOLUMNSKEY 'AuditKey',
                ROW_NUMBER() over (partition by pa.lpolicyKey,PA.lpolicyactivitykey order by DTDATEAMENDED) as ranked
        from policyactivity pa 
        inner join Auditheader AH on AH.LAUDITOBJECTINSTANCEKEY = pa.lPolicyActivityKey and AH.LAUDITOBJECTKEY = 611
        left join Auditrows AR on AR.LAUDITHEADERKEY=AH.LAUDITHEADERKEY and  AH.LAUDITOBJECTKEY = 611
        left join  Auditcolumns AC on AC.LAUDITROWSKEY=AR.LAUDITROWSKEY and AR.LAUDITOBJECTKEY = 611
        where  AH.LAUDITOBJECTKEY = 611 
        and  AR.LAUDITOBJECTKEY = 611 
        and AC.LENTITYPROPERTYKEY = 9008  -- expiry date
        -- contra endorsemenent
        and  pa.lTypeOfPolicyActivityKey = 8)contra_hist_ranked
    where ranked = 1


) 


select * from contra_hist
