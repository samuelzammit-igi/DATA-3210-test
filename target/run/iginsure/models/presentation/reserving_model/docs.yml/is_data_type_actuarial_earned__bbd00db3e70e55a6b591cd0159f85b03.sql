select
      
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
      select *
      from "IGI_PROD_DW"."dbt_szammit"."is_data_type_actuarial_earned__bbd00db3e70e55a6b591cd0159f85b03"
  
    ) dbt_internal_test