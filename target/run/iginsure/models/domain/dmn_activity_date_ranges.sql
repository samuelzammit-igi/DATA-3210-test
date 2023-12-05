
  
    
  
  if object_id ('"dbt_szammit"."dmn_activity_date_ranges__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dmn_activity_date_ranges__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dmn_activity_date_ranges__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dmn_activity_date_ranges__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dmn_activity_date_ranges__dbt_tmp_temp_view] as
    

with

dmn_policy_part as (
    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_policy_part"
),

ranked_activities_for_date as (
    select
        policy_id,
        activity_id,
        activity_written_timestamp,
        activity_notes,
        row_number() over (
            partition by policy_id, cast(activity_written_timestamp as Date)
            order by activity_id desc
        ) as rank
    from
        dmn_policy_part
    where
        activity_written_timestamp is not null
        and written_account_period is not null
),

max_activities_for_date as (
    select
        policy_id,
        activity_id,
        cast(activity_written_timestamp as Date) as activity_written_date
    from
        ranked_activities_for_date
    where
        rank = 1
),

activity_notes_for_date as (
    select 
        policy_id,
        cast(activity_written_timestamp as Date) as activity_written_date,
        string_agg(activity_notes, ''\n '') within group (order by activity_written_timestamp) as activity_notes
    from
        ranked_activities_for_date
    group by
        policy_id,
        activity_written_timestamp

),

date_ranges as (
    select
        policy_id,
        activity_id,
        activity_written_date as active_from_date,
        lead(activity_written_date) over (partition by policy_id order by activity_written_date) as active_until_date
    from
        max_activities_for_date
),

ranked_activities_for_period as (
    select
        policy_id,
        activity_id,
        --written_account_period_aus,
        written_account_period,
        row_number() over (
            partition by policy_id, written_account_period
            order by activity_id desc
        ) as rank

    from
        dmn_policy_part
    where
        activity_written_timestamp is not null
        and written_account_period is not null
),

max_activities_for_period as (
    select
        policy_id,
        activity_id,
        written_account_period
    from
        ranked_activities_for_period
    where
        rank = 1
),


period_ranges as (
    select
        policy_id,
        activity_id,
        written_account_period as active_from_period,
        lead(written_account_period) over (partition by policy_id order by written_account_period) as active_until_period
    from
        max_activities_for_period
),

activity_ranges as (
    select
        dmn_policy_part.policy_reference,
        dmn_policy_part.policy_id,
        dmn_policy_part.activity_id,
        dmn_policy_part.line_division as division,
        dmn_policy_part.line_subdivision as subdivision,
        dmn_policy_part.line_producing_office as producing_office,
        dmn_policy_part.activity_state,
        dmn_policy_part.policy_state,
        dmn_policy_part.activity_state_is_active,
        dmn_policy_part.activity_created,
        dmn_policy_part.activity_placement_code,
        dmn_policy_part.activity_placement_mop,
        dmn_policy_part.section_class,
        dmn_policy_part.line_product_name as product_name,
        dmn_policy_part.product_segregation_name,
        dmn_policy_part.segmentation,
        dmn_policy_part.budget_segmentation,
        dmn_policy_part.sub_class_segmentation,
        dmn_policy_part.producer_source_group_name,
        dmn_policy_part.line_producer as producer,
        dmn_policy_part.producer_group_name,
        dmn_policy_part.activity_type_of_insurance,
        dmn_policy_part.coverage_description,
        dmn_policy_part.coverage_code,
        dmn_policy_part.line_territory,
        dmn_policy_part.line_domicile,
        dmn_policy_part.line_region as region,
        dmn_policy_part.US_NonUS as region_Split,
        dmn_policy_part.mis_uw_year,
        dmn_policy_part.uw_year,
        dmn_policy_part.insured,
        dmn_policy_part.reassured,
        dmn_policy_part.file_handler,
        dmn_policy_part.underwriter,
        dmn_policy_part.recommended_underwriter,
        dmn_policy_part.admin_file_handler,
        dmn_policy_part.operator_id,
        dmn_policy_part.policy_inception,
        dmn_policy_part.policy_expiry,
        dmn_policy_part.policy_expiry_reported,
        dmn_policy_part.New_vs_Renwal,
        activity_notes_for_date.activity_notes,

        dmn_policy_part.written_account_period,
        dmn_policy_part.written_account_period / 100 as written_year,
        dmn_policy_part.written_account_period % 100 as written_month,
        dmn_policy_part.activity_period_from,
        dmn_policy_part.activity_period_to,
        date_ranges.active_from_date,
        date_ranges.active_until_date,
        period_ranges.active_from_period,
        period_ranges.active_until_period

    from
        dmn_policy_part
        left join date_ranges on
            dmn_policy_part.policy_id = date_ranges.policy_id
            and dmn_policy_part.activity_id = date_ranges.activity_id
        left join activity_notes_for_date on
            date_ranges.policy_id = activity_notes_for_date.policy_id
            and date_ranges.active_from_date = activity_notes_for_date.activity_written_date
        left join period_ranges on
            dmn_policy_part.policy_id = period_ranges.policy_id
            and dmn_policy_part.activity_id = period_ranges.activity_id
   
    where
        date_ranges.active_from_date is not null
        or period_ranges.active_from_period is not null
)

select * from activity_ranges
    ');

  CREATE TABLE "dbt_szammit"."dmn_activity_date_ranges__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dmn_activity_date_ranges__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dmn_activity_date_ranges__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dmn_activity_date_ranges__dbt_tmp_temp_view"
    end



  