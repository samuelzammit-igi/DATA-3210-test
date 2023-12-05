

select
    policy_id,
    count(distinct(claim_id)) as claim_ct,
    count(distinct(movement_date)) as movement_ct,
    count(*) as row_ct,
    sum(current_amount_usd) as current_amount_usd_sum,
    sum(historical_amount_usd) as historical_amount_usd_sum
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_gre_filtered"
group by policy_id