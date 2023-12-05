create view "dbt_dev"."stg_inception__dbt_tmp" as
    -- Get the inception and expiry per account period

with accountperiod as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_account_period" where _valid_to is null
),

policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy" where _valid_to is null
),


policyline as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_line" where _valid_to is null
),

policyactivity as (

     select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_activity" where _valid_to is null

),

AccountsPayableReceivable as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_apr" where _valid_to is null
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


AUDITSTATETRANSITIONS as (
  select 
        *
    from 
        "IGI_PROD_DW"."dbt_dev"."scd_audit_state_transitions"  
        where _valid_to is null

),


correct_written_account_period as (

    select  * from"IGI_PROD_DW"."dbt_dev"."stg_written_account_period"  

),

dates_per_account_period as (

    select *
    from(
    select a.lpolicykey,nyearperiod as AccountPeriod,dttransition 'AccountPeriod_ClosedDate',inceptiondate, row_number() over (partition by a.lpolicykey,nyearperiod order by auditkey desc) Rank 
    from
    (
      select * from(
            select distinct p.lpolicykey,pa.lpolicyactivitykey,p.dtperiodfrom,
                        --case when ap.nYearPeriod is null then (select max(account_period) + 1
                        --									  from dbt_ahmad.stg_account_periods_closing)
                        --									  else ap.nYearPeriod end as nYearPeriod,
                        isnull(ap.nYearPeriod,ap_activity.nYearPeriod) as nYearPeriod,
                        ISNULL(ast.DTTRANSITION,GETDATE()) AS DTTRANSITION, 
                        row_number() over (partition by pa.lpolicyactivitykey,ap.nYearPeriod
                        order by ast.DTTRANSITION desc) as ranked
            from policy p
            inner join policyactivity pa on p.lpolicykey =pa.lpolicykey
            left join correct_written_account_period correct_ap on correct_ap.activity_id = pa.lpolicyactivitykey
            left join accountperiod ap on correct_ap.account_period =ap.nYearPeriod
            left join AUDITSTATETRANSITIONS ast on ast.LINSTANCEKEY = ap.lAccountPeriodKey and ast.LENTITYKEY = 429 
            left join accountperiod ap_activity on ap_activity.laccountperiodkey = pa.lwrittenaccountperiodkey
            --where ast.LENTITYSTATEMEMBERKEY = 2087
            where p.bissequence = -1)all_activites
	  where ranked = 1		
    )a
    left join (
    select lpolicykey,inceptiondate,  isnull((lag(DTDATEAMENDED,1) OVER(PARTITION BY lpolicykey ORDER BY DTDATEAMENDED )),'2002-01-01')  active_from ,
    case when DTDATEAMENDED  = cast(GETDATE() as date) then null else DTDATEAMENDED end  active_until,auditkey
    from(
    --previous inception date
    select PA.LPOLICYKEY 'lpolicyKey',DTDATEAMENDED, DTOLDVALUE  'InceptionDate',LAUDITCOLUMNSKEY 'AuditKey'
    from 
    policyactivity pa 
    inner join [dbo].[AUDITHEADER] AH on AH.LAUDITOBJECTINSTANCEKEY = pa.lPolicyActivityKey and AH.LAUDITOBJECTKEY = 611
    left join [dbo].[AUDITROWS] AR on AR.LAUDITHEADERKEY=AH.LAUDITHEADERKEY and  AH.LAUDITOBJECTKEY = 611
    left join  [dbo].[AUDITCOLUMNS] AC on AC.LAUDITROWSKEY=AR.LAUDITROWSKEY and AR.LAUDITOBJECTKEY = 611
    where  AH.LAUDITOBJECTKEY = 611 
    and  AR.LAUDITOBJECTKEY = 611 
    and AC.LENTITYPROPERTYKEY in (6574)  


    union all

    -- latest inception date
    select lpolicykey,cast(getdate() as date) 'DTDATEAMENDED',dtperiodfrom 'InceptionDate'  , 100000000 'Auditkey'
    from policy
    --where lpolicyKey in (177)
    )c
    )b
    on a.lPolicyKey = b.lpolicyKey
    and  a.DTTRANSITION  BETWEEN COALESCE(b.active_from ,'2002-01-01')  AND COALESCE(b.active_until,getdate())
    )r
    where rank =1

)


SELECT * FROM dates_per_account_period
