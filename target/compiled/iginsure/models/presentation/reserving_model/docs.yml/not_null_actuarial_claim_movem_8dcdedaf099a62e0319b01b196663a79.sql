
    
    



select dur_loss_claims_made_to_reported
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_gre_agg_movement_duration"
where dur_loss_claims_made_to_reported is null


