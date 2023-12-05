-- transformations applied to the policy table in notebook
-- 100_Curate_InitialClean.ipynb



select
    policy_id,
    cast(uw_year as int) as uw_year,
    cast(mis_uw_year as date) as mis_uw_year,
    cast(product as varchar) as product,
    cast(class as varchar) as class,
    cast(sub_class as varchar) as sub_class,
    cast(coverage as varchar) as coverage,
    cast(coverage_code as varchar) as coverage_code,
    cast(lloyds_risk_code as varchar) as lloyds_risk_code,
    cast(insured as varchar) as insured,
    cast(reassured as varchar) as reassured,
    cast((
        case
            when direct_reinsurance_flag = 'direct' then 1
            when direct_reinsurance_flag = 'assumed' then 0
        end
        )
        as bit) as is_direct,
    cast(region as varchar) as region,
    cast(territory as varchar) as territory,
    cast(domicile_country as varchar) as domicile_country,
    cast(written_date as datetime) as written_date,
    cast(inception_date as datetime) as inception_date,
    cast(expiry_date as datetime) as expiry_date,
    cast(cancellation_date as datetime) as cancellation_date,
    cast(cancellation_type as varchar) as cancellation_type,
    cast(policy_status as varchar) as policy_status,
    cast(activity as varchar) as activity,
    cast((
        case
            when activity_new_vs_renewal = 'renewal' then 1
            when activity_new_vs_renewal = 'new' then 0
        end
        )
        as bit) as is_renewal,
    cast(activity_status as varchar) as activity_status,
    cast(placing_code as varchar) as placing_code,
    cast(placing_method as varchar) as placing_method,
    cast((
        case
            when claims_made_flag = 'YES' then 1
            when claims_made_flag = 'NO' then 0
        end
        )
        as bit) as is_claims_made,
    cast(working_line_pct as float) as working_line_pct,
    cast(gross_written_share as float) as gross_written_share,
    cast(reserving_class_1 as varchar) as reserving_class_1,
    cast(reserving_class_2 as varchar) as reserving_class_2,
    cast(reserving_class_3 as varchar) as reserving_class_3,
    cast(
        reserving_class_xol_allocation as varchar
    ) as reserving_class_xol_allocation,
    cast(account_period as int) as account_period,
    cast(tiv_usd as float) as tiv_usd,
    cast(tiv_share_usd as float) as tiv_share_usd,
    cast(tiv_100_usd as float) as tiv_100_usd,
    cast(event_limit_100_usd as float) as event_limit_100_usd,
    cast(pml_100_usd as float) as pml_100_usd,
    cast(pml_share_usd as float) as pml_share_usd
from "IGI_PROD_DW"."dbt_dev"."actuarial_policy_load"