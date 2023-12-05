select
      
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
      select *
      from "IGI_PROD_DW"."dbt_szammit"."is_data_type_actuarial_policy_clean_nb100_account_period__int"
  
    ) dbt_internal_test