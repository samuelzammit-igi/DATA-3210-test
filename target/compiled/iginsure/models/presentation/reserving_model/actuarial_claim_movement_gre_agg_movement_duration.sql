-- TODO: check if correct:
--          we assume that within a group, each categorical variable has the
--          same value throughout
--          we could use a CTE/partition to select the first one
--          (https://stackoverflow.com/a/3800572)
--          but MAX should work just fine to return that value, as there is no
--          order since all values are equal

--          MAX also works for movement_date_latest_in_dur since we want to pick
--          the latest (largest) date in the group
--          MAX also works for is_lockdown, where we want a value of TRUE if
--          there is at least one TRUE value in the group




select
    policy_id,
    claim_id,
    dur_inception_to_movement,
    cast(max(cast(is_lockdown as int)) as bit) as is_lockdown,
    max(loss_date) as loss_date,
    max(loss_date_claims_made) as loss_date_claims_made,
    max(reported_date) as reported_date,
    max(movement_date) as movement_date_latest_in_dur,
    max(dur_inception_to_loss) as dur_inception_to_loss,
    max(dur_inception_to_loss_claims_made) as dur_inception_to_loss_claims_made,
    max(dur_loss_to_reported) as dur_loss_to_reported,
    max(dur_loss_claims_made_to_reported) as dur_loss_claims_made_to_reported,
    sum(current_amount_usd_sum) as current_amount_usd_sum,
    sum(historical_amount_usd_sum) as historical_amount_usd_sum
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_gre_agg_movement_date"
group by policy_id, claim_id, dur_inception_to_movement