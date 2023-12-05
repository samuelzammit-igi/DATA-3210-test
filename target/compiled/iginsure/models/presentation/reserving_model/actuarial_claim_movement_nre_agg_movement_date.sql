-- TODO: check if correct:
--          we assume that within a group, each categorical variable has the
--          same value throughout
--          we could use a CTE/partition to select the first one
--          (https://stackoverflow.com/a/3800572)
--          but MAX should work just fine to return that value, as there is no
--          order since all values are equal




select
    policy_id,
    claim_id,
    movement_date,
    cast(max(cast(is_cat as int)) as bit) as is_cat,
    cast(max(cast(is_nat_cat as int)) as bit) as is_nat_cat,
    cast(max(cast(is_lockdown as int)) as bit) as is_lockdown,
    count(*) as row_ct,
    max(event_code) as event_code,
    max(event_type) as event_type,
    max(event_desc) as event_desc,
    max(loss_date) as loss_date,
    max(loss_date_claims_made) as loss_date_claims_made,
    max(reported_date) as reported_date,
    max(dur_inception_to_loss) as dur_inception_to_loss,
    max(dur_inception_to_loss_claims_made) as dur_inception_to_loss_claims_made,
    max(dur_loss_to_reported) as dur_loss_to_reported,
    max(dur_loss_claims_made_to_reported) as dur_loss_claims_made_to_reported,
    max(dur_loss_to_movement) as dur_loss_to_movement,
    max(dur_loss_claims_made_to_movement) as dur_loss_claims_made_to_movement,
    max(dur_inception_to_movement) as dur_inception_to_movement,
    sum(current_amount_usd) as current_amount_usd_sum,
    sum(historical_amount_usd) as historical_amount_usd_sum
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_nre_filtered"
group by policy_id, claim_id, movement_date