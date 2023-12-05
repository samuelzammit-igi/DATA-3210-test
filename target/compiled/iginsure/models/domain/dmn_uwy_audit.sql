with 
policyactivity as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_activity" where _valid_to is null
),
auditheader as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_auditheader" where _valid_to is null
),

AUDITROWS as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_auditrows" where _valid_to is null
),
AUDITCOLUMNS as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_auditcolumns" where _valid_to is null
),
policyline as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_line" where _valid_to is null
),

Policy_Activity_Audit as (
select  PA.LPOLICYKEY 'lpolicyKey',PA.lPolicyActivityKey,DTDATEAMENDED, DOLDVALUE  'UW Year',LAUDITCOLUMNSKEY 'AuditKey'
from policyactivity pa 
left join auditheader ah on pa.lPolicyActivityKey = ah.LAUDITOBJECTINSTANCEKEY and ah.LAUDITOBJECTKEY = 611
left join AUDITROWS ar on ar.LAUDITHEADERKEY = ah.LAUDITHEADERKEY and ar.LAUDITOBJECTKEY =2286
left join AUDITCOLUMNS ac on ac.LAUDITHEADERKEY =ar.LAUDITHEADERKEY
where
LENTITYPROPERTYKEY = 2001379 --dtuwyear
),
Policy_Activity_Current as (
	select pl.lpolicykey 'lpolicyKey',pl.lPolicyActivityKey, getdate() DTDATEAMENDED, dtuwyear 'UW Year',1000000000 'AuditKey'
	from policyline pl
),
PA_UW_List as 
(
	select * from Policy_Activity_Audit

	union all

	select * from Policy_Activity_Current
)
select lpolicykey,lpolicyactivitykey,[uw year],  lag(DTDATEAMENDED,1) OVER(partition by lpolicyactivitykey ORDER BY DTDATEAMENDED )  active_from ,
case when cast(DTDATEAMENDED as date) = cast(GETDATE() as date) then null else DTDATEAMENDED end  active_until,auditkey 
from  PA_UW_List