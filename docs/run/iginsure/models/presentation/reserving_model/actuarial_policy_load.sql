
  
    
  
  if object_id ('"dbt_szammit"."actuarial_policy_load__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_policy_load__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."actuarial_policy_load__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."actuarial_policy_load__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[actuarial_policy_load__dbt_tmp_temp_view] as
    

with pol
as (
    select *
    from "IGI_PROD_DW"."dbt_szammit"."actuarial_all_core_filtered"
)

select
    policyref as policy_id,
    product as product,
    uwyear as uw_year,
    mis_uw_year as mis_uw_year,
    class as class,
    sub_class as sub_class,
    coverage as coverage,
    coveragecd as coverage_code,
    lloydsriskcode as lloyds_risk_code,
    insured as insured,
    reassured as reassured,
    directreinsuranceindicator as direct_reinsurance_flag,
    region as region,
    territory as territory,
    domicilecountry as domicile_country,
    datewritten as written_date,
    policyinceptiondate as inception_date,
    policyexpirydate as expiry_date,
    cancellationdate as cancellation_date,
    cancellationtype as cancellation_type,
    policystatus as policy_status,
    activity as activity,
    activity_new_vs_renewal as activity_new_vs_renewal,
    activitystatus as activity_status,
    placing as placing_code,
    mop as placing_method,
    claims_made_flag as claims_made_flag,
    [Sum of WorkingLinePer] as working_line_pct,
    [Sum of GrossWrittenShare] as gross_written_share,
    reservingclass1 as reserving_class_1,
    reservingclass2 as reserving_class_2,
    reservingclass3 as reserving_class_3,
    reservingclass_xol_allocation as reserving_class_xol_allocation,
    accountperiod as account_period,
    [Sum of TotalSumInsuredCCY1$] as tiv_usd,
    [Sum of TotalSumInsuredShareCCY1$] as tiv_share_usd,
    [Sum of TotalSumInsured100$] as tiv_100_usd,
    [Sum of EventLimit100$] as event_limit_100_usd,
    [Sum of PML100$] as pml_100_usd,
    [Sum of PMLShare$] as pml_share_usd
from pol
    ');

  CREATE TABLE "dbt_szammit"."actuarial_policy_load__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[actuarial_policy_load__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."actuarial_policy_load__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_policy_load__dbt_tmp_temp_view"
    end



  