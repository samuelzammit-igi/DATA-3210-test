select
      
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
      select *
      from "IGI_PROD_DW"."dbt_szammit"."is_data_type_actuarial_policy__3f4f3a937a33ec19df19f052da225caf"
  
    ) dbt_internal_test