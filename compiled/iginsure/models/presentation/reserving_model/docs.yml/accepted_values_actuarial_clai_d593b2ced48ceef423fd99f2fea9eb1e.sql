
    
    

with all_values as (

    select
        reinsurance_type as value_field,
        count(*) as n_records

    from "IGI_PROD_DW"."dbt_dev"."actuarial_claim_movement_clean_nb110"
    group by reinsurance_type

)

select *
from all_values
where value_field not in (
    'XOL','QS','FAC','Gross'
)


