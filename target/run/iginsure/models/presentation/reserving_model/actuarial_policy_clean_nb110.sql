
  
    
  
  if object_id ('"dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[actuarial_policy_clean_nb110__dbt_tmp_temp_view] as
    -- transformations applied to the policy table in notebook
-- 110_Curate_FeatEng_Elements.ipynb




with calculating_policy_duration_days as (
    select
        *,
        cast(uw_year as int) as uw_y,
        concat(''s'', cast(uw_year as varchar)) as uw_ys,
        datediff(dayofyear, inception_date, expiry_date) as policy_duration_days,
        (2022 - year(mis_uw_year)) as years_since_mis_uw_year
    from "IGI_PROD_DW"."dbt_szammit"."actuarial_policy_clean_nb100"
)

select
    *,
    case
        when policy_duration_days in (364, 365, 366) then ''1yr''
        when policy_duration_days < 364 then ''<1yr''
        when policy_duration_days > 366 then ''>1yr''
    end as policy_duration_grp
from calculating_policy_duration_days
    ');

  CREATE TABLE "dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[actuarial_policy_clean_nb110__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_policy_clean_nb110__dbt_tmp_temp_view"
    end



  