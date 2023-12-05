create view "dbt_dev"."stg_cancellation_reason_history__dbt_tmp" as
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

account_period_closing as (

    select * from "IGI_PROD_DW"."dbt_dev"."stg_account_periods_closing"

),

cancellation_reason_hist  as (

    select * from(

    select pa.lpolicyKey,PA.lpolicyactivitykey 'lpolicyactivitykey',DTDATEAMENDED, DOLDVALUE 'lTypeOfCancellationReasonKey',
          LAUDITCOLUMNSKEY 'AuditKey',ROW_NUMBER() over (partition by pa.lpolicyKey,pa.lpolicyactivitykey,DTDATEAMENDED
                                                        ,DOLDVALUE,LAUDITCOLUMNSKEY order by ap.date_closed) as ranked,ap.account_period 
                                                        as cancellation_reason_ap
    from policyactivity pa 
    inner join [dbo].[AUDITHEADER] AH on AH.LAUDITOBJECTINSTANCEKEY = pa.lPolicyActivityKey and AH.LAUDITOBJECTKEY = 611
    left join [dbo].[AUDITROWS] AR on AR.LAUDITHEADERKEY=AH.LAUDITHEADERKEY and  AH.LAUDITOBJECTKEY = 611
    left join  [dbo].[AUDITCOLUMNS] AC on AC.LAUDITROWSKEY=AR.LAUDITROWSKEY and AR.LAUDITOBJECTKEY = 611
    inner join account_period_closing ap on DTDATEAMENDED  <= ap.date_closed
    where  AH.LAUDITOBJECTKEY = 611 
    and  AR.LAUDITOBJECTKEY = 611 
    and AC.LENTITYPROPERTYKEY = 44874  -- cancellationtype

    )hist 
    
    where ranked = 1
    
    )

select * from cancellation_reason_hist
