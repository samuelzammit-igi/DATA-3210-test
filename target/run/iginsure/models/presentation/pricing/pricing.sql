
  
    
  
  if object_id ('"dbt_szammit"."pricing__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."pricing__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."pricing__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."pricing__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[pricing__dbt_tmp_temp_view] as
    with latest_activity_id as (

    select * from "IGI_PROD_DW"."dbt_szammit"."stg_latest_activity_per_policy"

),


latest_activity_id_renewal as (


    -- for every renewal policy 
    -- in the renewal policy id, get the latest activity
    -- so in the cte , you have the renewal policy id with its latest activity

    -- the renewal policy id will be the same for all  the acitivtes

    select * from "IGI_PROD_DW"."dbt_szammit"."stg_latest_activity_per_renewal_policy"

),

dmn_inward_claim as (

    select * from "IGI_PROD_DW"."dbt_szammit"."dmn_claim"

),

dmn_policy_part as (

    -- mapping certain lobs to match with business plan
    select *, case when line_product_name in (''INWARDS PPN'',''MR'',''INWARDS XOL'') then ''reinsurance''
                   when line_product_name in (''MARINE TRADE'',''MARINE LIABILITY'') then ''marine''
                   when line_product_name = ''FORESTRY'' then ''property''
                   when line_product_name = ''FINANCIAL INSTITUTIONS'' then ''fi''
                   else lower(line_product_name) end as line_product_name_used
     from "IGI_PROD_DW"."dbt_szammit"."dmn_policy_part" policy_part


     where policy_state not in (''Submission'',''Declined'',''Quote'',''NTU'',''Bound'',''Submission is Void'')

     and bIsSequence = -1

),


renewal_policy_latest as (

    -- get the latest activity related to a renewal policy , so when joining with the policy part ,
        -- the result will have the latest activity per renewal policy
    
    select * 
    from dmn_policy_part policy_part
    inner join latest_activity_id_renewal as latest_per_policy on latest_per_policy.latest_activity_id = policy_part.activity_id


),


stg_apr as (

    select * from "IGI_PROD_DW"."dbt_szammit"."stg_apr"

),


apr_policy_line as (
    select
        dmn_policy_part.policy_id,
        gross_igi_share_base as iw_gross,
        acq_cost_base as iw_acq,
        stg_apr.period_year_period
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
        gross_igi_share_base as iw_gross,
        acq_cost_base as iw_acq,
        stg_apr.period_year_period
    from
        stg_apr
        inner join dmn_inward_claim on
            stg_apr.apr_instance_id = dmn_inward_claim.claim_movement_id
        inner join dmn_policy_part on
            dmn_inward_claim.policy_line_id = dmn_policy_part.line_id
    where
        stg_apr.apr_entity_type = ''Claim Movement''
        and (is_gross_igi_share = 1 or is_acq_cost = 1)
),


apr_records as (
    
  select policy_id,
         period_year_period,
         sum(iw_gross) as iw_gross,
         sum(iw_acq) as iw_acq
    from (  
    select * from apr_policy_line

    union all

    select * from apr_claim_movement)iw_g_by_policy

    group by policy_id,period_year_period


),


dmn_policy_part_renewal as (

    -- when you join with the domain policy part that has already latest activity
    -- join it based on the renewal policy id = policy id from the renewal cte


    -- by account period, by renwal policy , so when you join with the pricing cte on orgi policy id, renewal policy id , account period,
    -- and written account period

    select apr_records.period_year_period,
           policy_part.policy_id,
           policy_part.policy_id_renewal,
           apr_records.iw_gross/(case when policy_part.igi_sgn_line = 0 then 1 else policy_part.igi_sgn_line end) as actual_gwp_100,
           apr_records.iw_gross * policy_part.igi_sgn_line as actual_gwp_igi_sgn_line
    from     
    apr_records 
    inner join 
        (select policy_part.policy_id as policy_id, 
                renewal_policy_latest.policy_id as policy_id_renewal,
                 renewal_policy_latest.igi_sgn_line
        from dmn_policy_part policy_part
        inner join latest_activity_id as latest_per_policy on latest_per_policy.latest_activity_id = policy_part.activity_id
        inner join  renewal_policy_latest on renewal_policy_latest.policy_id = policy_part.renewal_policy_id)policy_part 
    on apr_records.policy_id = policy_part.policy_id_renewal

 
),

--pricing_renewal (

  --  select *

    --from apr_records

    --inner join renewal_policy_latest on apr_records.policy_id = policy_part.policy_id

--),


pricing as (

    select 
            *,
        -- iw_gross amounts , techincal , and plan percentages for each pricing types
          

-- run it in execute mode

        --case when pricing_type = ''UW'' then sum(case when pricing_type = ''UW'' then actual_gwp_100 else 0 end)
          --                                               over(partition by lob, Reserving_Class_2, pricing_type) end as UW_actual_gwp_100,

        case when pricing_type = ''UW'' then actual_gwp_igi_sgn_line else 0 end as UW_actual_gwp_igi_sgn_line ,

        --case when pricing_type = ''UW'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as UW_plan_premium_percentage,

        --case when pricing_type = ''UW'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as UW_technical_premium_percentage,

        --case when pricing_type = ''ACTUARY'' then sum(case when pricing_type = ''ACTUARY'' then actual_gwp_100 else 0 end)
          --                                               over(partition by lob, Reserving_Class_2, pricing_type) end as ACTUARY_actual_gwp_100,

        case when pricing_type = ''ACTUARY'' then actual_gwp_igi_sgn_line else 0 end as ACTUARY_actual_gwp_igi_sgn_line ,

        --case when pricing_type = ''ACTUARY'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as ACTUARY_plan_premium_percentage,

        --case when pricing_type = ''ACTUARY'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as ACTUARY_technical_premium_percentage,

        --case when pricing_type = ''Model'' then sum(case when pricing_type = ''Model'' then actual_gwp_100 else 0 end)
          --                                               over(partition by lob, Reserving_Class_2, pricing_type) end as Model_actual_gwp_100,

        case when pricing_type = ''Model'' then actual_gwp_igi_sgn_line else 0 end as Model_actual_gwp_igi_sgn_line ,

        --case when pricing_type = ''Model'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as Model_plan_premium_percentage,

        --case when pricing_type = ''Model'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as Model_technical_premium_percentage,

        --case when pricing_type = ''Incomplete'' then sum(case when pricing_type = ''Incomplete'' then actual_gwp_100 else 0 end)
          --                                               over(partition by lob, Reserving_Class_2, pricing_type) end as Incomplete_actual_gwp_100,

        case when pricing_type = ''Incomplete'' then actual_gwp_igi_sgn_line else 0 end as Incomplete_actual_gwp_igi_sgn_line ,

        --case when pricing_type = ''Incomplete'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as Incomplete_plan_premium_percentage,

        --case when pricing_type = ''Incomplete'' then sum(actual_gwp_igi_sgn_line) over (partition by Reserving_Class_2) /
          --                                               sum() over (partition by Reserving_Class_2) 
            --                                             end as Incomplete_technical_premium_percentage

            (actual_gwp_100 ) * plan_adequacy  as plan_premium_100,

            (actual_gwp_100) * technical_adequacy as technical_premium_100,
            
            -- plan premium_100 * igi sgn line 
            (actual_gwp_100 * plan_adequacy) * igi_sgn_line as plan_premium_igi_sgn_line,

            -- technical premium_100 * igi sgn line 
            (actual_gwp_100 * technical_adequacy) * igi_sgn_line as technical_premium_igi_sgn_line,

            SUM(CASE WHEN pricing_type IS NOT NULL THEN actual_gwp_100 ELSE 0 END)
            OVER (PARTITION BY Reserving_Class_2) AS actual_gwp_all_types,


            NULL AS pricing_gross_lr

            from 
            
            (select pricing_base.* ,
                    renewal.policy_id_renewal,
                    renewal.actual_gwp_100 as actual_gwp_100_renewal,
                    renewal.actual_gwp_igi_sgn_line as actual_gwp_igi_sgn_line_renewal

             from
                (select 
                
                policy_part.policy_id,

                policy_part.policy_reference,

                replace(ltrim(rtrim(policy_part.line_product_name_used)),'' '', '''') as line_product_name_used ,

                policy_part.sub_class_segmentation,

                policy_part.activity_placement_mop as mop,

                policy_part.coverage_code,

                policy_part.Classification,

                policy_part.line_producing_office,

                policy_part.insured,

                policy_part.underwriter,

                policy_part.renewal_policy_id,

                --policy_part.Sub_Class,

                CASE WHEN policy_part.line_division =  1 THEN ''London''   
                    WHEN policy_part.line_division = 0 THEN ''Bermuda'' 
                    ELSE ''Eurpoe'' END AS division,

                policy_part.line_subdivision as subdivision,

                replace(ltrim(rtrim(policy_part.line_product_name_used)),'' '', '''') as lob,

                replace(lower(ltrim(rtrim(policy_part.Reserving_Class_2))),'' '', '''') as Reserving_Class_2,

                CONCAT(replace(ltrim(rtrim(policy_part.line_product_name_used)),'' '', '''') ,''_'',replace(lower(ltrim(rtrim(policy_part.Reserving_Class_2))),'' '', ''''),''_'',policy_part.uw_year) AS lob_resclass2_year,

                apr_records.period_year_period,

                eomonth(DATEFROMPARTS(LEFT(apr_records.period_year_period,4),RIGHT(apr_records.period_year_period,2),1)) AS AccountPeriod_date,

                policy_part.igi_sgn_line,

                policy_part.pricing_type,

                policy_part.pricing_time,

                policy_part.uw_year,

                policy_part.policy_inception,

                policy_part.policy_expiry,

                policy_part.plan_adequacy,

                policy_part.technical_adequacy,

                policy_part.lModelSequencekey,

                apr_records.iw_gross/(case when policy_part.igi_sgn_line = 0 then 1 else policy_part.igi_sgn_line end) as actual_gwp_100,

                apr_records.iw_gross * policy_part.igi_sgn_line as actual_gwp_igi_sgn_line


                from apr_records
                inner join dmn_policy_part as policy_part on apr_records.policy_id = policy_part.policy_id
                inner join latest_activity_id as latest_per_policy on latest_per_policy.latest_activity_id = policy_part.activity_id


                where apr_records.period_year_period >=202004)pricing_base
                
                left join dmn_policy_part_renewal renewal on renewal.policy_id_renewal = pricing_base.renewal_policy_id
                                                          and renewal.policy_id = pricing_base.policy_id
                                                          and renewal.period_year_period = pricing_base.period_year_period

                )renewal_info
)

select * from pricing
    ');

  CREATE TABLE "dbt_szammit"."pricing__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[pricing__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."pricing__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."pricing__dbt_tmp_temp_view"
    end



  