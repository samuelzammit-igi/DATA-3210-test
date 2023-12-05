select
      
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
      select *
      from "IGI_PROD_DW"."dbt_szammit"."is_data_type_actuarial_claim_m_af5486349332a1ba47a20bf05a50f7e0"
  
    ) dbt_internal_test