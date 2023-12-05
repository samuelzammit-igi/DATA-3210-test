-- transformations applied to the claim movement table in notebook
-- 100_Curate_InitialClean.ipynb




select
    policy_id,
    claim_id,
    cast(movement_date as date) as movement_date,
    cast(reinsurance_type as varchar) as reinsurance_type,
    cast(loss_date as date) as loss_date,
    cast(loss_date_claims_made as date) as loss_date_claims_made,
    cast(reported_date as date) as reported_date,
    cast(event_code as varchar) as event_code,
    cast(event_type as varchar) as event_type,
    cast(event_desc as varchar) as event_desc,
    cast(claim_desc as varchar) as claim_desc,
    cast((case
            when cat_flag = 'Y' then 1
            else 0
        end) as bit) as is_cat,
    cast((case
            when nat_cat_flag = 'Y' then 1
            else 0
        end) as bit) as is_nat_cat,
    cast(org_ccy_code as varchar) as org_ccy_code,
    cast(fx_rate_ccy_per_usd as float) as fx_rate_ccy_per_usd,
    cast(amount_type as varchar) as amount_type,
    cast(historical_amount_usd as float) as historical_amount_usd,
    cast(current_amount_usd as float) as current_amount_usd,
    cast(original_amount_ccy as float) as original_amount_ccy,
    cast(policy_inception_date_yq as int) as policy_inception_date_yq,
    cast(claim_reporting_date_yq as int) as claim_reporting_date_yq,
    cast(claim_loss_date_yq as int) as claim_loss_date_yq,
    cast(
        claim_loss_date_claims_made_yq as int
    ) as claim_loss_date_claims_made_yq,
    cast(
        policy_inception_date_dev_dur_q as int
    ) as policy_inception_date_dev_dur_q,
    cast(
        claim_reporting_date_dev_dur_q as int
    ) as claim_reporting_date_dev_dur_q,
    cast(
        claim_loss_date_dev_dur_q as int
    ) as claim_loss_date_dev_dur_q,
    cast(
        claim_loss_date_claims_made_dev_dur_q as int
    ) as claim_loss_date_claims_made_dev_dur_q
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_load"