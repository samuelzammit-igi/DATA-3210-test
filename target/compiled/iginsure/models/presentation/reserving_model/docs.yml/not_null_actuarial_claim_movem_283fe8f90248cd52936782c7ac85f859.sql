
    
    



select current_amount_usd_sum
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_nre_agg_movement_duration"
where current_amount_usd_sum is null


