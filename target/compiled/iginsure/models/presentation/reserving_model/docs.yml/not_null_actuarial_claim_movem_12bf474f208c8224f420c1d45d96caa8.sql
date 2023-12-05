
    
    



select dur_inception_to_loss_claims_made
from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_gre_agg_movement_duration"
where dur_inception_to_loss_claims_made is null


