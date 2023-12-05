
  
    
  
  if object_id ('"dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[stg_auth_mvmt_pending_financial__dbt_tmp_temp_view] as
    --

with

claim_movement as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_claim_movement"
),

account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_account_period"
),

current_account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_current_account_period"
),

entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_entity_instance_states"
	where entity_type = ''Claim Movement''
), 

apr as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_apr"
    where apr_type  = ''inward_claim''
),
journal_pair as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_journal_pairs"
    where journal_pair_entity_type  = ''Claim Movement''
),

auth_mvmt_pending_financials as(
    SELECT
	 CM.lClaimMovementKey
	FROM 
	 claim_movement CM 
	 INNER JOIN account_period			Acc_Period ON CM.lAccountPeriodKey = Acc_Period.lAccountPeriodKey
	 INNER JOIN entity_instance_states	EISCM ON CM.lClaimMovementKey = EISCM.instance_id
	WHERE
	 CM.bManualMovement = 0
	 and CM.lTypeOfMovementKey NOT IN(10,21) /*Reserve Amendment,Initial Reserve*/
	 and EISCM.entity_state_id in( 2262,2263) /*Authorized, Paid*/
	 and Acc_Period.nYearPeriod<= (select ap from current_account_period)
	 and NOT EXISTS(SELECT apr_reference FROM apr WHERE apr_instance_id = CM.lClaimMovementKey)
	 and NOT EXISTS(SELECT journal_pair_id FROM journal_pair WHERE journal_pair_instance_id = CM.lClaimMovementKey)
	 and CM.sPaymentReference IS NULL

)

select * from  auth_mvmt_pending_financials
    ');

  CREATE TABLE "dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[stg_auth_mvmt_pending_financial__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."stg_auth_mvmt_pending_financial__dbt_tmp_temp_view"
    end



  