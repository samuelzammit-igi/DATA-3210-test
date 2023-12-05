
    
    

select
    (policy_id + '-' + claim_id + '-' + convert(varchar,movement_date) + '-' + reinsurance_type  + '-' + amount_type + '-' + org_ccy_code) as unique_field,
    count(*) as n_records

from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_load"
where (policy_id + '-' + claim_id + '-' + convert(varchar,movement_date) + '-' + reinsurance_type  + '-' + amount_type + '-' + org_ccy_code) is not null
group by (policy_id + '-' + claim_id + '-' + convert(varchar,movement_date) + '-' + reinsurance_type  + '-' + amount_type + '-' + org_ccy_code)
having count(*) > 1


