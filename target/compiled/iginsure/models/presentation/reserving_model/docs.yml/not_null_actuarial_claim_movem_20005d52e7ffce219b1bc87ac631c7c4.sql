
    
    



select historical_amount_usd_sum
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_nre_agg_movement_duration"
where historical_amount_usd_sum is null


