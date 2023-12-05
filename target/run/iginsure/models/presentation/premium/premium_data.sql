
  
    
  
  if object_id ('"dbt_szammit"."premium_data__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."premium_data__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."premium_data__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."premium_data__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[premium_data__dbt_tmp_temp_view] as
    
with

dmn_activity_date_ranges as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_activity_date_ranges"
),

dmn_policy_part as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_policy_part"
),

stg_claim_movement as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_claim_movement"
),

stg_apr as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_apr"
),

dmn_iris_mvmts as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_iris_mvmts"
),

dmn_ri_policy_part as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_ri_policy_part"
),

apr_policy_line as (
    select
        dmn_policy_part.policy_id,
        0 as ri_section_broker_security_id,
        dmn_policy_part.activity_id as apr_activity_id,
        dmn_policy_part.activity_id as effective_activity_id,
        stg_apr.apr_reference,
        stg_apr.period_year_period,
        stg_apr.period_year,
        stg_apr.period_month,
        cast (stg_apr.entry_date as date) as entry_date,
        cast (
            case
                when stg_apr.entry_date < dmn_policy_part.activity_written_timestamp
                    then dmn_policy_part.activity_written_timestamp
                else stg_apr.entry_date
            end
        as date) as effective_date,
        stg_apr.allocation_date,
        stg_apr.due as settlement_due_date,
        
        stg_apr.base_amount,

        gross_igi_share_base as gross_premium,
        acq_cost_base as acq,

        stg_apr.apr_state,
        stg_apr.type_of_monetary_amount,
        ''INWARD'' as iw_ow_flag,

        stg_apr.orig_amount,
        stg_apr.currency,
        stg_apr.apr_roe,
        stg_apr.gross_igi_share_org as gross_premium_org,
        stg_apr.acq_cost_org as acq_org
    from
        stg_apr
        inner join dmn_policy_part on
            stg_apr.apr_instance_id = dmn_policy_part.line_id
    where
        stg_apr.apr_entity_type = ''Policy Line''
),

apr_claim_movement as (
    select
        dmn_policy_part.policy_id,
        0 as ri_section_broker_security_id,
        dmn_policy_part.activity_id as apr_activity_id,
        dmn_policy_part.activity_id as effective_activity_id,
        stg_apr.apr_reference,
        stg_apr.period_year_period,
        stg_apr.period_year,
        stg_apr.period_month,
        cast (stg_apr.entry_date as date) as entry_date,
        cast (stg_apr.entry_date as date) as effective_date,
        stg_apr.allocation_date,
        stg_apr.due as settlement_due_date,
        
        stg_apr.base_amount,

        gross_igi_share_base as gross_premium,
        acq_cost_base as acq,

        stg_apr.apr_state,
        stg_apr.type_of_monetary_amount,
        ''INWARD'' as iw_ow_flag,

        stg_apr.orig_amount,
        stg_apr.currency,
        stg_apr.apr_roe,
        stg_apr.gross_igi_share_org as gross_premium_org,
        stg_apr.acq_cost_org as acq_org
    from
        stg_apr
        inner join stg_claim_movement on
            stg_apr.apr_instance_id = stg_claim_movement.claim_movement_id
        inner join dmn_policy_part on
            stg_claim_movement.policy_activity_id = dmn_policy_part.activity_id
    where
        stg_apr.apr_entity_type = ''Claim Movement''
        and (is_gross_igi_share = 1 or is_acq_cost = 1)
),

apr_fac_movement as (
    select 
        dmn_policy_part.policy_id,
        dmn_ri_policy_part.ri_section_broker_security_id,
        dmn_policy_part.activity_id as apr_activity_id,
        dmn_policy_part.activity_id as effective_activity_id,
        stg_apr.apr_reference,
        stg_apr.period_year_period,
        stg_apr.period_year,
        stg_apr.period_month,
        cast (stg_apr.entry_date as date) as entry_date,
        cast (
            case
                when stg_apr.entry_date < dmn_policy_part.activity_written_timestamp
                    then dmn_policy_part.activity_written_timestamp
                else stg_apr.entry_date
            end
        as date) as effective_date,
        stg_apr.allocation_date,
        stg_apr.due as settlement_due_date,
        
        stg_apr.base_amount,

        gross_igi_share_base as gross_premium,
        acq_cost_base as acq,

        stg_apr.apr_state,
        stg_apr.type_of_monetary_amount,
        ''OF'' as iw_ow_flag,

        stg_apr.orig_amount,
        stg_apr.currency,
        stg_apr.apr_roe,
        stg_apr.gross_igi_share_org as gross_premium_org,
        stg_apr.acq_cost_org as acq_org
    from 
     stg_apr
        inner join dmn_policy_part on
            stg_apr.source_instance_id = dmn_policy_part.line_id
            inner join dmn_ri_policy_part on 
                stg_apr.apr_instance_id = dmn_ri_policy_part.ri_section_broker_security_id
    where
        stg_apr.apr_type = ''outward_premium''
        and dmn_ri_policy_part.type_of_ri_policy_code = ''OF''
),

apr_fac_obligatory_movement as (
    select 
        dmn_policy_part.policy_id,
        dmn_ri_policy_part.ri_section_broker_security_id,
        dmn_policy_part.activity_id as apr_activity_id,
        dmn_policy_part.activity_id as effective_activity_id,
        stg_apr.apr_reference,
        stg_apr.period_year_period,
        stg_apr.period_year,
        stg_apr.period_month,
        cast (stg_apr.entry_date as date) as entry_date,
        cast (
            case
                when stg_apr.entry_date < dmn_policy_part.activity_written_timestamp
                    then dmn_policy_part.activity_written_timestamp
                else stg_apr.entry_date
            end
        as date) as effective_date,
        stg_apr.allocation_date,
        stg_apr.due as settlement_due_date,

        stg_apr.base_amount,
        
        gross_igi_share_base as gross_premium,
        acq_cost_base as acq,

        stg_apr.apr_state,
        stg_apr.type_of_monetary_amount,
        ''FT'' as iw_ow_flag,

        stg_apr.orig_amount,
        stg_apr.currency,
        stg_apr.apr_roe,
        stg_apr.gross_igi_share_org as gross_premium_org,
        stg_apr.acq_cost_org as acq_org
    from 
     stg_apr
        inner join dmn_policy_part on
            stg_apr.source_instance_id = dmn_policy_part.line_id
            inner join dmn_ri_policy_part on 
                stg_apr.apr_instance_id = dmn_ri_policy_part.ri_section_broker_security_id
    where
        stg_apr.apr_type = ''outward_premium''
        and dmn_ri_policy_part.type_of_ri_policy_code = ''FT''
),

apr_qs_movement as (
    select 
        dmn_policy_part.policy_id,
        dmn_ri_policy_part.ri_section_broker_security_id,
        dmn_policy_part.activity_id as apr_activity_id,
        dmn_policy_part.activity_id as effective_activity_id,
        stg_apr.apr_reference,
        stg_apr.period_year_period,
        stg_apr.period_year,
        stg_apr.period_month,
        cast (stg_apr.entry_date as date) as entry_date,
        cast (
            case
                when stg_apr.entry_date < dmn_policy_part.activity_written_timestamp
                    then dmn_policy_part.activity_written_timestamp
                else stg_apr.entry_date
            end
        as date) as effective_date,
        stg_apr.allocation_date,
        stg_apr.due as settlement_due_date,

        stg_apr.base_amount,
        
        gross_igi_share_base as gross_premium,
        acq_cost_base as acq,

        stg_apr.apr_state,
        stg_apr.type_of_monetary_amount,
        ''QS'' as iw_ow_flag,

        stg_apr.orig_amount,
        stg_apr.currency,
        stg_apr.apr_roe,
        stg_apr.gross_igi_share_org as gross_premium_org,
        stg_apr.acq_cost_org as acq_org
    from 
     stg_apr
        inner join dmn_policy_part on
            stg_apr.source_instance_id = dmn_policy_part.line_id
            inner join dmn_ri_policy_part on 
                stg_apr.apr_instance_id = dmn_ri_policy_part.ri_section_broker_security_id
    where
        stg_apr.apr_type = ''outward_premium''
        and dmn_ri_policy_part.type_of_ri_policy_code in (''OQ'',''OS'')
),

apr_records as (
    select * from apr_policy_line

    union all

    select * from apr_claim_movement

    union all 

    select * from apr_fac_movement

    union all 

    select * from apr_fac_obligatory_movement

    union all 

    select * from apr_qs_movement
),

policy_account_period_activities as (
    select
        apr_records.policy_id as policy_id,
        apr_records.apr_reference,
        apr_records.apr_activity_id,
        apr_records.base_amount as apr_amount_usd,
        apr_records.gross_premium,
        apr_records.acq,
        
        --original amounts
        apr_records.orig_amount as apr_amount_org,
        apr_records.currency as apr_org_ccy,
        apr_records.apr_roe,
        apr_records.gross_premium_org,
        apr_records.acq_org,
        
        apr_records.entry_date as apr_entry_date,
        --apr_records.allocation_date as apr_allocation_date,
        apr_records.settlement_due_date as apr_settlement_due_date,
        apr_records.period_year_period as apr_account_period,
        apr_records.period_month as AP_month,
        apr_records.period_year as AP_year,
        cast(datefromparts(apr_records.period_year,apr_records.period_month ,1) as date) as AP_Date,
        --apr_records.apr_state as apr_status,
        apr_records.type_of_monetary_amount,

        activity.activity_version,
        activity.activity_type,
        activity.activity_id as effective_activity_id,
        activity.line_division as activity_division,
        activity.line_subdivision as subdivision,
        activity.line_product_name as product,
        activity.sub_class_segmentation as SubClassSegmentation,
        activity.product_segregation_name as product_segregation,
        activity.coverage_description as activity_coverage_description,
        activity.coverage_code as activity_coverage_code,
        activity.section_class as activity_section_class,
        activity.written_account_period as activity_written_account_period,
        activity.policy_reference,
        activity.activity_source,
        activity.broker_version_number,

        apr_records.iw_ow_flag,
        ri.ri_policy,
        ri.ri_policy_inception,
        ri.ri_policy_expiry,
        ri.ow_broker,
        ri.ow_broker_major_group,
        ri.ow_security,

        for_date.activity_id as for_date_activity_id,
        for_date.division as for_date_division,
        for_date.subdivision as subdivision_day,
        for_date.producing_office as ProducingOffice_day,
        for_date.product_name as for_date_product_name,
        for_date.product_segregation_name as ProductSegregation_day,
        for_date.segmentation as segmentation_day,
        for_date.budget_segmentation as BudgetSegmentation_day,
        for_date.sub_class_segmentation as SubClassSegmentation_day,
        for_date.section_class as Class_day,
        for_date.producer_source_group_name as ProducerSource_day,
        for_date.producer as producer_day,
        for_date.producer_group_name as ProducerGroup_day,
        for_date.activity_type_of_insurance as InsuranceType_day,
        for_date.coverage_description as coverageDesc_day,
        for_date.coverage_code as  CoverageCode_day,
        for_date.line_territory as territory_day,
        for_date.line_domicile as  domicile_day,
        for_date.region as region_day,
        for_date.mis_uw_year as MISUWY_day,
        for_date.uw_year as UWY_day,
        for_date.insured as insured_day,
        for_date.reassured as reassured_day,
        for_date.file_handler as FileHandler_day,
        for_date.underwriter as  underwriter_day,
        for_date.recommended_underwriter as RecUnderwriter_day,
        for_date.admin_file_handler as AdminFileHandler_day,
        for_date.operator_id as OperatorId_day,
        for_date.activity_notes as activity_notes_day,
        for_date.policy_inception as for_date_policy_inception,
        for_date.policy_expiry as for_date_policy_expiry,
        for_date.policy_expiry_reported as for_date_policy_expiry_reported,
        for_date.New_vs_Renwal as for_date_New_vs_Renwal,
        
        for_period.activity_id as for_period_activity_id,
        for_period.division as for_period_division,
        for_period.subdivision as for_period_subdivision,
        for_period.producing_office as ProducingOffice_month,
        for_period.product_name as for_period_product_name,
        for_period.product_segregation_name as ProductSegregation_month,
        for_period.segmentation as segmentation_month,
        for_period.budget_segmentation as BudgetSegmentation_month,
        for_period.sub_class_segmentation as SubClassSegmentation_month,
        for_period.section_class as class_month,
        for_period.producer_source_group_name as ProducerSource_month,
        for_period.producer as producer_month,
        for_period.producer_group_name as ProducerGroup_month,
        for_period.activity_type_of_insurance as InsuranceType_month,
        for_period.coverage_description as CoverageDesc_month,
        for_period.coverage_code as CoverageCode_month,
        for_period.line_territory as territory_month,
        for_period.line_domicile as  domicile_month,
        for_period.region as region_month,
        for_period.region_split as region_split_month,
        for_period.mis_uw_year as MISUWY_month,
        for_period.uw_year as UWY_month,
        for_period.insured as insured_month,
        for_period.reassured as reassured_month,
        for_period.file_handler as FileHandler_month,
        for_period.underwriter as underwriter_month,
        for_period.recommended_underwriter as RecUnderwriter_month,
        for_period.admin_file_handler as AdminFileHandler_month,
        for_period.operator_id as OperatorId_month,
        for_period.activity_notes as activity_notes_period,
        for_period.policy_inception as for_period_policy_inception,
        for_period.policy_expiry as for_period_policy_expiry,
        for_period.policy_expiry_reported as for_period_policy_expiry_reported,
        for_period.New_vs_Renwal as for_period_New_vs_Renwal

    from
        apr_records
        inner join dmn_policy_part as activity on
            apr_records.policy_id = activity.policy_id
            and apr_records.effective_activity_id = activity.activity_id
            and activity.written_account_period is not null
        left join dmn_activity_date_ranges as for_date on
            apr_records.policy_id = for_date.policy_id
            and apr_records.effective_date >= for_date.active_from_date
            and apr_records.effective_date < coalesce(for_date.active_until_date, getdate())
        left join dmn_activity_date_ranges as for_period on
            apr_records.policy_id = for_period.policy_id
            and apr_records.period_year_period >= for_period.active_from_period
            and apr_records.period_year_period < coalesce(for_period.active_until_period, 300000)
        --outward
        left join dmn_ri_policy_part ri on 
        apr_records.ri_section_broker_security_id = ri.ri_section_broker_security_id
        --and apr_records.iw_ow_flag <> ''INWARD''

        WHERE  apr_records.period_year_period >=202004
),


combined_premium_data as (

SELECT *,
    concat(product,'','',SubClassSegmentation) as Product_Subclass_Segm_For_Join
 FROM 

    (
        
        select * from policy_account_period_activities

    union all

        select * from dmn_iris_mvmts
    
    ) final_prem

)

select * from combined_premium_data
    ');

  CREATE TABLE "dbt_szammit"."premium_data__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      HEAP
      )
    AS (SELECT * FROM [dbt_szammit].[premium_data__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."premium_data__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."premium_data__dbt_tmp_temp_view"
    end



  