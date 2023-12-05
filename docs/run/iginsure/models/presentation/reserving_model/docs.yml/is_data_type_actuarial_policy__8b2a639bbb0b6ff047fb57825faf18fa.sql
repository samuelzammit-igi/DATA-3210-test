select
      
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
      select *
      from "IGI_PROD_DW"."dbt_szammit"."is_data_type_actuarial_policy__8b2a639bbb0b6ff047fb57825faf18fa"
  
    ) dbt_internal_test