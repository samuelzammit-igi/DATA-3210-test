
  
    
  
  if object_id ('"dbt_szammit"."provisioned_cmr__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."provisioned_cmr__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."provisioned_cmr__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."provisioned_cmr__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[provisioned_cmr__dbt_tmp_temp_view] as
    

with
claims_financial  as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_claim_amounts"
),

claims_non_financial as (
    select distinct claim_id,parent_claim_reference from "IGI_PROD_DW"."dbt_szammit"."dmn_claim"
),

policy_non_financial as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_policy_part"
),

Current_account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_current_account_period"
),

Previous_YTD_Account_Period as (
    select concat(left(AP,4)-1,''12'') ''Prev_AP'' from Current_account_period
),
Claims_ASAT_YTD as (
    select * from "IGI_PROD_DW"."dbo"."ClaimsASAT"
    where accountperiod = (select * from Previous_YTD_Account_Period)   
),
Claims_ASAT_Current as (
    select cnf.parent_claim_reference, cf.sCcy,cf.TypeOfRI
    --,pnf.mis_uw_year as MISUWY ,line_product_name as Product,line_subdivision as SubDivision
    ,
    -- OS
    Sum(TotalOSOrg) TotalOSOrg,	sum(TotalOsAcCcy) TotalOsAcCcy,	sum(LastOSAmountOrgCcyExpense) TotalOSOrgCcyExp , 	sum(LastOSAmountAcCcyExpense) TotalOSAcCcyExp ,
    sum(LastOSAmountOrgIND) TotalOSOrgCcyIND,	sum(LastOSAmountACCcyIND) TotalOSACCcyIND,
    --Paid
    sum(TotalPytsRcptsOrg)TotalPaidOrg,	sum(TotalPytsRcptsAcCcy) TotalPaidAcCcy,sum(LastPytsRcptsAmountOrgExpense) TotalPaidOrgExp, sum(LastPytsRcptsAmountACCcyExpense) TotalPaidACCcyExp,
    sum(LastPytsRcptsAmountOrgIND) TotalPaidOrgIND,	sum(LastPytsRcptsAmountACCcyIND) TotalPaidACCcyIND,
    -- incurred
    sum(TotalIncurredOrgCcy)TotalIncurredOrgCcy, sum(TotalIncurredACCcy) TotalIncurredACCcy, sum(LastIncurredOrgExpense) TotalIncurredOrgExp, sum(LastIncurredACCcyExpense) TotalIncurredACCcyExp,
    sum(LastIncurredOrgIND) TotalIncurredOrgIND, sum(LastIncurredACCcyIND)  TotalIncurredACCcyIND

    from claims_financial cf 
    left join claims_non_financial cnf on cf.claim_id = cnf.claim_id
    --left join policy_non_financial pnf on cnf.policy_id = pnf.policy_id and cnf.policy_activity_id = pnf.activity_id

    group by cnf.parent_claim_reference,cf.sCcy,cf.TypeOfRI
    --,pnf.mis_uw_year ,line_product_name ,line_subdivision
)
select * from  Claims_ASAT_Current
    ');

  CREATE TABLE "dbt_szammit"."provisioned_cmr__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      HEAP
      )
    AS (SELECT * FROM [dbt_szammit].[provisioned_cmr__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."provisioned_cmr__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."provisioned_cmr__dbt_tmp_temp_view"
    end



  