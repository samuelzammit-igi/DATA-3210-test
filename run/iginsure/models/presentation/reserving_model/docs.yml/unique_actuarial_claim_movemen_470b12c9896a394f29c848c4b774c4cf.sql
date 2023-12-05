select
      
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
      select *
      from "IGI_PROD_DW"."dbt_szammit"."unique_actuarial_claim_movemen_470b12c9896a394f29c848c4b774c4cf"
  
    ) dbt_internal_test