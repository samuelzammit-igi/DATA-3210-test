
    
    

select
    policy_id as unique_field,
    count(*) as n_records

from "IGI_PROD_DW"."dbt_dev"."actuarial_policy_load"
where policy_id is not null
group by policy_id
having count(*) > 1


