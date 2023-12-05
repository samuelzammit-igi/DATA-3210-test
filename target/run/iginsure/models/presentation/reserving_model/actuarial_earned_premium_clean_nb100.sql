
  
    
  
  if object_id ('"dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[actuarial_earned_premium_clean_nb100__dbt_tmp_temp_view] as
    -- transformations applied to the premium earning table in notebook
-- 100_Curate_InitialClean.ipynb



with premium as (
    select * from "IGI_PROD_DW"."dbt_szammit"."actuarial_earned_premium_load"
),

renaming as (
    select
        policy_id,
        earning_qtr,
        cast(is_earned as bit) as is_earned,
        cast(gross_earned_premium as float) as gross_earned_premium,
        cast(inward_acquisition_cost as float) as gross_earned_premium_acq,
        --TODO confirm that there are no agency fees
        cast(quota_share_premium as float) as qs_earned_premium,
        cast(quota_share_acquisition as float) as qs_earned_premium_acq,
        cast(fac_premium as float) as fac_earned_premium,
        cast(fac_acquisition as float) as fac_earned_premium_acq,
        cast(allocated_xol_cost as float) as xol_premium_allocated_acq
    from premium
),

adding_xol_flag as (
    select
        policy_id,
        earning_qtr,
        is_earned,
        cast((
            case
                when xol_premium_allocated_acq is null then 0
                else 1
            end
        )
        as bit) as is_xol,
        gross_earned_premium,
        gross_earned_premium_acq,
        qs_earned_premium,
        qs_earned_premium_acq,
        fac_earned_premium,
        fac_earned_premium_acq,
        isnull(xol_premium_allocated_acq, 0) as xol_premium_allocated_acq 
    from renaming
)

select * from adding_xol_flag
    ');

  CREATE TABLE "dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[actuarial_earned_premium_clean_nb100__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_earned_premium_clean_nb100__dbt_tmp_temp_view"
    end



  