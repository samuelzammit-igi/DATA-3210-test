-- transformations applied to the claim movement table in
-- notebook 110_Curate_FeatEng_Elements.ipynb
-- TODO the notebook removes inception_date ... is this necessary?




with

cm as (
    select * from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_clean_nb100"
),

pol as (
    select * from "IGI_PROD_DW"."dbt_dev"."actuarial_policy_clean_nb100"
)

select
    pol.*,
    cm.claim_id,
    cm.movement_date,
    cm.reinsurance_type,
    cm.loss_date,
    cm.loss_date_claims_made,
    cm.reported_date,
    cm.event_code,
    cm.event_type,
    cm.event_desc,
    cm.claim_desc,
    cm.is_cat,
    cm.is_nat_cat,
    cm.org_ccy_code,
    cm.fx_rate_ccy_per_usd,
    cm.amount_type,
    cm.historical_amount_usd,
    cm.current_amount_usd,
    cm.original_amount_ccy,
    cm.policy_inception_date_yq,
    cm.claim_reporting_date_yq,
    cm.claim_loss_date_yq,
    cm.claim_loss_date_claims_made_yq,
    cm.policy_inception_date_dev_dur_q,
    cm.claim_reporting_date_dev_dur_q,
    cm.claim_loss_date_dev_dur_q,
    cm.claim_loss_date_claims_made_dev_dur_q,
    cast((case
            when (cm.movement_date >= '2020-04-01')
                and (cm.movement_date <= '2021-08-01') then 1
            else 0
        end) as bit) as is_lockdown,
    datediff(
        quarter, cm.loss_date, cm.reported_date
    ) as dur_loss_to_reported,
    datediff(
        quarter, cm.loss_date_claims_made, cm.reported_date
    ) as dur_loss_claims_made_to_reported,
    datediff(
        quarter, cm.loss_date, cm.movement_date
    ) as dur_loss_to_movement,
    datediff(
        quarter, cm.loss_date_claims_made, cm.movement_date
    ) as dur_loss_claims_made_to_movement,
    datediff(
        quarter, pol.inception_date, cm.loss_date
    ) as dur_inception_to_loss,
    datediff(
        quarter, pol.inception_date, cm.loss_date_claims_made
    ) as dur_inception_to_loss_claims_made,
    datediff(
        quarter, pol.inception_date, cm.movement_date
    ) as dur_inception_to_movement

from cm
left join pol
    on cm.policy_id = pol.policy_id