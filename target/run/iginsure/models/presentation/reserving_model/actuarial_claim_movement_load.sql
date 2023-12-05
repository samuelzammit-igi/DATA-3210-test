
  
    
  
  if object_id ('"dbt_szammit"."actuarial_claim_movement_load__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_claim_movement_load__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."actuarial_claim_movement_load__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."actuarial_claim_movement_load__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[actuarial_claim_movement_load__dbt_tmp_temp_view] as
    

with pol (policy_id)
as (
    select policyref as policy_id
    from "IGI_PROD_DW"."dbt_szammit"."actuarial_all_core_filtered"
),

cm as (
    select * from "IGI_PROD_DW"."dbo"."ActuarialCopy_ClaimsData_Unpivoted"
)

select
    pol.policy_id,
    cm.claimreference as claim_id,
    cm.control_date as movement_date,
    cm.reinsurance_type as reinsurance_type,
    cm.dateoflossfrom_org as loss_date,
    cm.dateoflossfrom_claimsmade as loss_date_claims_made,
    cm.adviseddate as reported_date,
    cm.eventcd as event_code,
    cm.eventtype as event_type,
    cm.eventdesc as event_desc,
    cm.claimdescription as claim_desc,
    cm.cat_indicator as cat_flag,
    cm.nat_cat_indicator as nat_cat_flag,
    cm.ccy as org_ccy_code,
    cm.fx as fx_rate_ccy_per_usd,
    cm.amount_type as amount_type,
    cm.hist_amount_accccy as historical_amount_usd,
    cm.current_amount_accccy as current_amount_usd,
    cm.org_amount as original_amount_ccy,
    cm.orig_uw_q as policy_inception_date_yq,
    cm.orig_r_q as claim_reporting_date_yq,
    cm.orig_a_q as claim_loss_date_yq,
    cm.orig_ar_q as claim_loss_date_claims_made_yq,
    cm.dev_uw_qq as policy_inception_date_dev_dur_q,
    cm.dev_r_qq as claim_reporting_date_dev_dur_q,
    cm.dev_a_qq as claim_loss_date_dev_dur_q,
    cm.dev_ar_qq as claim_loss_date_claims_made_dev_dur_q
from pol
inner join cm
    on pol.policy_id = cm.policyref
-- use the control date variable to filter the list of claims and
-- get a snapshot of the claims data as-at the control date
where convert(varchar, cm.control_date, 112) -- YYYYMMDD
    <= ''20230823''
    ');

  CREATE TABLE "dbt_szammit"."actuarial_claim_movement_load__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[actuarial_claim_movement_load__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."actuarial_claim_movement_load__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_claim_movement_load__dbt_tmp_temp_view"
    end



  