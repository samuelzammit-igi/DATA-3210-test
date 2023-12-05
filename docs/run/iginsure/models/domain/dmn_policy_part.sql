
  
    
  
  if object_id ('"dbt_dev"."dmn_policy_part__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_dev"."dmn_policy_part__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_dev"."dmn_policy_part__dbt_tmp"','U') is not null
    begin
    drop table "dbt_dev"."dmn_policy_part__dbt_tmp"
    end


   EXEC('create view [dbt_dev].[dmn_policy_part__dbt_tmp_temp_view] as
    

with

policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."dmn_policy" where _valid_to is null
),

policy_activity as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_activity" where _valid_to is null
),

policy_line as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_line" where _valid_to is null
),

type_of_policy_line as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_policy_line" where _valid_to is null
),

policy_section as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_section" where _valid_to is null
),

type_of_class as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_class" where _valid_to is null
),

sub_class as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_sub_class" where _valid_to is null
),

type_of_placement as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_placement" where _valid_to is null
),

stg_entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_entity_instance_states"
),

audit_state_transitions_full as (
    select 
        *
     from 
        "IGI_PROD_DW"."dbt_dev"."stg_activity_audit_state_transitions" 
),

type_of_pricing as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_pricing" where _valid_to is null
),

type_of_pricing_time as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_pricing_time" where _valid_to is null
),

account_period_closing as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_account_periods_closing"
),

cancellation_reason_hist as (

    select * from 
        (select *,row_number() over(partition by lpolicyactivitykey,lTypeOfCancellationReasonKey 
                                    order by DTDATEAMENDED ) as changes_ranked 
        from "IGI_PROD_DW"."dbt_dev"."stg_cancellation_reason_history" )final
    where lTypeOfCancellationReasonKey = 2000001 -- NA
    and changes_ranked = 1

),


contra_hist as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_contra_expiry_history" 
),


audit_state_transitions as (
    select
        LINSTANCEKEY,
        CAST(min(DTTRANSITION) AS DATE) as DTTRANSITION,
        min(DTTRANSITION)  as DTTRANSITION_for_account_period

    from
        audit_state_transitions_full
    where
        (
            "LENTITYSTATEMEMBERKEY" in (
                2153, -- ''Written''
                2220, -- ''Cancellation''
                2204, -- ''Cancelled''
                2533, -- ''Cancelled Adjustment''
                2162, -- ''Cancelled Endorsement''
                2392, -- ''Cancelled Contra''
                2391  -- ''Contra Applied''
            )
            or CAST("DTTRANSITION" AS DATE)  = ''2019-03-16'' -- Migrated
        )
    group by
        LINSTANCEKEY
),

version_audit_trail as (
    select
        LENTITYINSTANCEKEY,
        LPREVIOUSENTITYINSTANCEKEY
    from
        "IGI_PROD_DW"."dbt_dev"."scd_version_audit_trail"
    where
        LENTITYKEY = 611 -- Policy Activity
        and _valid_to is null
),

stg_product as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_product"
),

type_of_insurance as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_insurance" where _valid_to is null
),

stg_subdivision as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_subdivision"
),

contact as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_contact"
),

stg_type_of_profit_centre as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_type_of_profit_centre"
),

type_of_policy_activity as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_policy_activity"
),

type_of_activity_source as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_activity_source"
),

stg_country_territory as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_country_territory"
),

stg_division as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_division"
),

stg_account_period as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_account_period"
),

stg_coverage as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_coverage"
),

segmentation as (
    select * from "IGI_PROD_DW"."dbo"."Segmentation"
),

classification as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_classification" where _valid_to is null
),

stg_broker_group as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_broker_group"
),

security_users as(
    select * from "IGI_PROD_DW"."dbo"."securityusers"
),


inception_per_account_period as (

    select * from "IGI_PROD_DW"."dbt_dev"."stg_inception" 
),


activity_operator_id as(
    select 
     au.lInstanceKey,au.lSecurityUserKey,su.sFullName
    from 
     audit_state_transitions_full as au 
     inner join (select 
                    inner_AU.LINSTANCEKEY,MIN(inner_AU.DTTRANSITION) MinDTTRANSITION 
                 from 
                    audit_state_transitions_full inner_au
                 group by  inner_au.LINSTANCEKEY)Min_TransitionDate on au.lInstanceKey = Min_TransitionDate.lInstanceKey and au.DTTRANSITION = Min_TransitionDate.MinDTTRANSITION
	 inner join security_users su on au.lsecurityuserkey = su.lsecurityuserkey
),

activity_effective_written_timestamp as (
    select
        version_audit_trail.LENTITYINSTANCEKEY as activity_id,
        version_audit_trail.LPREVIOUSENTITYINSTANCEKEY as previous_activity_id,
        coalesce (
            audit_state_transitions.DTTRANSITION,
            previous_audit_state_transitions.DTTRANSITION
        ) as effective_written_timestamp,
        coalesce (
            audit_state_transitions.DTTRANSITION_for_account_period,
            previous_audit_state_transitions.DTTRANSITION_for_account_period
        ) as effective_written_timestamp_for_ap

    from
        version_audit_trail
        left join audit_state_transitions on
            version_audit_trail.LENTITYINSTANCEKEY = audit_state_transitions.LINSTANCEKEY
        left join audit_state_transitions as previous_audit_state_transitions on
            version_audit_trail.LPREVIOUSENTITYINSTANCEKEY = previous_audit_state_transitions.LINSTANCEKEY
),

correct_written_account_period as (

select * from     
    (select *,ROW_NUMBER() over (partition by activity_ts.activity_id order by ap.date_closed) as ranked
    from 
    activity_effective_written_timestamp activity_ts
    inner join account_period_closing ap on 
    activity_ts.effective_written_timestamp_for_ap  <= ap.date_closed)final
where ranked = 1

),

policy_part as (
    
    SELECT final.*,
           
           COALESCE(inception_per_account_period.inceptiondate,inception_from_policy) as policy_inception,

                    
            case when final.cancellation_reason_ap is null and final.contra_DTDATEAMENDED is null then final.policy_expiry
                 when final.cancellation_reason_ap is null and final.contra_DTDATEAMENDED is not null then final.policy_expiry_contra
                 when final.cancellation_reason_ap is not null and final.contra_DTDATEAMENDED is null 
                      and final.written_account_period < final.cancellation_reason_ap then final.policy_expiry_to
                 else final.policy_expiry end as policy_expiry_reported -- after the NA has changed
           
           FROM 
    
    (select
        policy.sReference as policy_reference,
        policy_activity."lPolicyKey" as policy_id,
        policy_line.llinkrenewalpolicykey as renewal_policy_id,
        policy_activity."lPolicyActivityKey" as activity_id,
        policy_activity."nversion" as activity_version,
        type_of_policy_activity."sactivity" as activity_type, 
        policy_line."lPolicyLineKey" as line_id,
        policy_section."lPolicySectionKey" as section_id,
        policy_activity."sPolicyNo" as policy_no,
        policy_activity."sUMR" as UMR,
        activity_effective_written_timestamp.previous_activity_id,
        stg_division.division as line_division,
        cancellation_reason_hist.cancellation_reason_ap,

        type_of_activity_source.sSearchTypeofActivitySourceDescr as activity_source,
        policy_activity.nBrokerVersionNumber as broker_version_number,

        -- written account period from the  policyactivity itself
       -- written_account_period.period_year_period as written_account_period,
        
        -- account period based on the written timestamp of each activity
        case when written_account_period.period_year_period is null then null
             else isnull(correct_ap.account_period,written_account_period.period_year_period) end as written_account_period,

        policy_activity.dtPeriodToDisplay as policy_expiry,

        policy_activity.dtPeriodTo as policy_expiry_to,

        contra_hist.DTDATEAMENDED as contra_DTDATEAMENDED,

        contra_hist.Expirydate as policy_expiry_contra,

        policy.dtPeriodFrom as inception_from_policy,
        
        policy_activity.dtPeriodFrom as inception_from_policy_activity,

        stg_entity_instance_states.entity_state as activity_state,
        policy_entity_instance_states.entity_state as policy_state,
        sub_class.sSubClassCode as SubClassCode,
        sub_class.sSubClassDescr AS Sub_Class,
        classification.sClassificationDescr as Classification,
        policy.bIsSequence,

        CASE 
    WHEN LEFT(policy.sReference,1) IN (''9'', ''8'') THEN ''Outward''
    WHEN  stg_product.product_name = ''DOWNSTREAM ENERGY'' THEN ''DownstreamEnergy''
    WHEN  stg_product.product_name = ''UPSTREAM ENERGY'' THEN ''UpstreamEnergy''
    WHEN LEFT(policy.sReference, 6) = ''100006'' THEN ''K6''
    WHEN LEFT(policy.sReference, 6) = ''600494'' THEN ''CasualtyPen'' 
    WHEN sub_class.sSubClassDescr IN (''MOTOR WHOLE ACCOUNT'', ''MEDICAL EXPENSES XL'') THEN ''InwardMedMotorEXP''
    WHEN  stg_product.product_name = ''INWARDS PPN'' THEN ''InwardPPN''
    WHEN  stg_product.product_name = ''INWARDS XOL'' THEN ''InwardXL''
    WHEN type_of_placement."sMOP" = ''Inwards Reinsurance XOL'' THEN ''InwardXL'' 
    WHEN type_of_placement."sMOP" = ''Inwards Reinsurance Proportional'' THEN ''InwardPPN''
    WHEN  stg_product.product_name = ''FORESTRY'' THEN ''PropertyForestry''
    WHEN  stg_product.product_name = ''PROPERTY'' THEN ''PropertyexForestry''					
    WHEN  stg_product.product_name = ''PORTS & TERMINALS'' THEN ''PortsTerminals''
    WHEN  stg_product.product_name = ''FINANCIAL INSTITUTIONS'' THEN 
        CASE WHEN  stg_coverage.coverage_code IN (''F1'',''F3'', ''PI'', ''DO'', ''F6'') THEN ''FIDOEO''
        ELSE ''FIBBBCC'' END 
    WHEN  stg_product.product_name = ''ENGINEERING'' THEN 
        CASE WHEN  stg_coverage.coverage_code IN (''CG'', ''CO'') THEN ''EngineeringCAR''
        ELSE 
        CASE WHEN  stg_coverage.coverage_code IN (''EAR PD'', ''EAR PD + BI'', ''EAR TPL (C.M.)'' ,''EAR BI'', ''EAR (OCC.)'', ''ER'', ''TP'', ''BI'', ''MB'')
        OR ( stg_coverage.coverage_code = ''DS'' AND classification.sClassificationDescr IN (''COMMERCE INDUSTRY'', ''OIL & GAS''))
        THEN ''EngineeringEAR''
        WHEN  stg_coverage.coverage_code IN (''ID'',''IB'') THEN ''EngineeringIDI''
        WHEN  stg_coverage.coverage_code IN (''DO'', ''TE'', ''PI'', ''PD'', ''PR'', ''S1'', ''OB'', ''MM'', ''GL'', ''WO'', ''PL'', ''EP'', ''G2'')
        THEN ''EngineeringOTHER''
        ELSE ''EngineeringCAR'' END END
    WHEN  stg_coverage.coverage_code = ''LE'' THEN 
        CASE WHEN RIGHT(sub_class.sSubClassDescr, 3) = ''ATE'' THEN ''CasualtyATE'' 
        ELSE ''CasualtyBTE'' END 
    WHEN  stg_product.product_name = ''Casualty'' THEN
        CASE WHEN  stg_coverage.coverage_code = ''DO'' THEN ''CasualtyComDO''
        WHEN  stg_coverage.coverage_code = ''WI'' THEN ''CasualtyWI''
        WHEN  stg_coverage.coverage_code = ''SU'' THEN ''Surety''
        ELSE ''CasualtyOther'' END
    --WHEN  stg_coverage.coverage_code = ''PI'' THEN ''CasualtyProf''

    WHEN  stg_product.product_name = ''MARINE LIABILITY'' THEN ''MarineLiability''
    WHEN  stg_product.product_name = ''MARINE TRADE'' THEN ''MarineTrade''
    WHEN  stg_product.product_name = ''MARINE'' AND classification.sClassificationDescr = ''MARINE LIABILITY'' THEN ''MarineLiability''
    WHEN  stg_product.product_name = ''MARINE'' THEN ''MarineCargo''

    --WHEN  stg_product.product_name = ''Reinsurance'' THEN 
    --	CASE WHEN mop = ''I/W L/S'' THEN ''InwardLS'' END 
        
    WHEN  stg_product.product_name = '''' OR  stg_product.product_name = ''All'' THEN ''Excluded''
    WHEN  stg_product.product_name = ''POLITICAL VIOLENCE'' THEN ''PoliticalViolence''
    WHEN  stg_product.product_name = ''CONTINGENCY'' THEN ''Contingency''
    ELSE  stg_product.product_name END AS Reserving_Class_2,

        case
            when stg_entity_instance_states.entity_state in (
                ''Endorsement Setup'',
                ''Cancelled Endorsement'',
                ''Cancellation Adjustment'',
                ''Cancellation Not Required'',
                ''Pending Contra'',
                ''Cancelled Contra'',
                ''Cancelled Adjustment''
            )
            then cast(''FALSE'' as bit)
            else cast(''TRUE'' as bit)
        end as activity_state_is_active,
        policy_activity."dtCreated" as activity_created,
        activity_effective_written_timestamp.effective_written_timestamp as activity_written_timestamp,
        policy_activity."dtPeriodFrom" as activity_period_from,
        policy_activity."dtPeriodTo" as   activity_period_to,
        type_of_placement."sCode" as activity_placement_code,
        type_of_placement."sMOP" as activity_placement_mop,
        type_of_class."SCLASS" as section_class,
        stg_product.product_id as line_product_id,
        stg_product.product_name as line_product_name,
        case
            when stg_product.product_id = 2000013 and type_of_class.SCLASS in (''CAR'',''EIO CAR'') then 101
            when stg_product.product_id = 2000013 and type_of_class.SCLASS not in (''CAR'',''EIO CAR'') then 2000013
            when stg_product.product_id = 2000014 and type_of_class.SCLASS not in (''POWER GENERATION-CONVENTIONAL'',''POWER GENERATION-RENEWABLES'') then 102
            when stg_product.product_id = 2000014 and type_of_class.SCLASS in (''POWER GENERATION-CONVENTIONAL'',''POWER GENERATION-RENEWABLES'') then 103
            else stg_product.product_id
        end as product_segregation_id,
        case
            when stg_product.product_name in (''UPSTREAM ENERGY'') and type_of_class.SCLASS in (''CAR'',''EIO CAR'') then ''UPSTREAM CONSTRUCTION''
            when stg_product.product_name in (''UPSTREAM ENERGY'') and type_of_class.SCLASS not in (''CAR'',''EIO CAR'') then ''UPSTREAM ENERGY''
            when stg_product.product_name in (''DOWNSTREAM ENERGY'') and type_of_class.SCLASS in (''POWER GENERATION-CONVENTIONAL'',''POWER GENERATION-RENEWABLES'') then ''POWER & RENEWABLES''
            when stg_product.product_name in (''DOWNSTREAM ENERGY'') and type_of_class.SCLASS not in (''POWER GENERATION-CONVENTIONAL'',''POWER GENERATION-RENEWABLES'') then ''OIL & GAS''
            else stg_product.product_name
        end as product_segregation_name,
        zone_country_territory.country as line_territory,
        zone_country_domicile.country as line_domicile,

        case
            when stg_product.product_name in (
                ''AVIATION'',''DOWNSTREAM ENERGY'',''FINANCIAL INSTITUTIONS'',''FORESTRY'',''INWARDS PPN'',
                ''INWARDS XOL'',''MARINE LIABILITY'',''MARINE TRADE'',''MR'',''POLITICAL VIOLENCE'',
                ''PORTS & TERMINALS'',''PROPERTY'',''UPSTREAM ENERGY'',''CONTINGENCY''
            )
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name)

            when stg_product.product_name = ''ENGINEERING'' and stg_coverage.first_coverage_description in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name and seg.Coverage = stg_coverage.first_coverage_description)
            when stg_product.product_name = ''ENGINEERING'' and stg_coverage.second_coverage_description in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name and seg.Coverage = stg_coverage.second_coverage_description)

            when stg_product.product_name = ''ENGINEERING'' and stg_coverage.first_coverage_description not in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name and seg.Coverage not in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI''))
            when stg_product.product_name = ''ENGINEERING'' and stg_coverage.second_coverage_description not in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name and seg.Coverage not in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI''))

            when stg_product.product_name = ''CASUALTY''
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name and seg.Coverage in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description))

            when stg_product.product_name = ''MARINE''
            then (select distinct [Grouping] from segmentation seg where seg.Product = stg_product.product_name and seg.Classification = classification.sClassificationDescr)
        end as segmentation,

        case
            when stg_product.product_name in (
                ''AVIATION'',''FINANCIAL INSTITUTIONS'',''INWARDS PPN'',''INWARDS XOL'',''MARINE LIABILITY'',''MARINE TRADE'',
                ''POLITICAL VIOLENCE'',''PORTS & TERMINALS'',''PROPERTY'',''CONTINGENCY''
            )
            then stg_product.product_name

            when stg_product.product_name = ''CASUALTY''
            then
                case
                    when ''PROFESSIONAL INDEMNITY'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description) and 
                    (substring(policy.sReference,1,6) <> ''600494'' and substring(policy.sReference,1,6) <> ''011122'' )
                    then ''PROFESSIONAL INDEMNITY''

                    when ''PROFESSIONAL INDEMNITY'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description) and 
                    (substring(policy.sReference,1,6) = ''600494'' or substring(policy.sReference,1,6) = ''011122'' ) 
                    then ''PEN''
                    when stg_coverage.first_coverage_description = ''WARRANTY AND INDEMNITY''
                    then ''WARRANTY & INDEMNITY''
                    when stg_coverage.second_coverage_description = ''WARRANTY AND INDEMNITY''
                    then ''WARRANTY & INDEMNITY''
                    when REPLACE(stg_coverage.first_coverage_description,''&amp;'',''&'') in (''DIRECTORS & OFFICERS'',''PUBLIC OFFERING OF SECURITIES INSURANCE'')
                    then ''DIRECTORS & OFFICERS''
                    when REPLACE(stg_coverage.second_coverage_description,''&amp;'',''&'') in (''DIRECTORS & OFFICERS'',''PUBLIC OFFERING OF SECURITIES INSURANCE'')
                    then ''DIRECTORS & OFFICERS''

                    when stg_coverage.first_coverage_description in(''MEDICAL MALPRACTICE'')
                    then stg_coverage.first_coverage_description
                    when stg_coverage.second_coverage_description in(''MEDICAL MALPRACTICE'')
                    then stg_coverage.second_coverage_description
                    
                    
                    when ''LEGAL EXPENSES'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description)
                    AND   sub_class.sSubClassDescr like ''%BTE%'' 
                    then ''LEGAL EXPENSES BTE''

                    when ''LEGAL EXPENSES'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description)
                    AND   sub_class.sSubClassDescr like ''%ATE%'' 
                    then ''LEGAL EXPENSES ATE''

                    when ''SURETY BONDS'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description)
                    then ''FINANCIAL INSTITUTIONS''
                    
                    else ''CGL''
                end

            when stg_product.product_name = ''DOWNSTREAM ENERGY''
            then
                case
                    when type_of_class.SCLASS in (''POWER GENERATION-CONVENTIONAL'',''POWER GENERATION-RENEWABLES'')
                    then ''POWER & RENEWABLES''
                    else ''OIL & GAS''
                end

            when stg_product.product_name = ''ENGINEERING''
            then
                case
                    when stg_coverage.first_coverage_description in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
                    then ''IDI''
                    when stg_coverage.second_coverage_description in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
                    then ''IDI''
                    else ''ENGINEERING''
                end

            when stg_product.product_name = ''FORESTRY''
            then ''PROPERTY''

            when stg_product.product_name = ''MARINE''
            then ''MARINE CARGO''

            when stg_product.product_name = ''UPSTREAM ENERGY''
            then
                case
                    when type_of_class.SCLASS in (''CAR'',''EIO CAR'')
                    then ''UPSTREAM CONSTRUCTION''
                    else ''UPSTREAM ENERGY''
                end

            when stg_product.product_name = ''MR''
            then ''INWARDS XOL''
        end as budget_segmentation,

        case
            when stg_product.product_name in (
                ''AVIATION'',''FINANCIAL INSTITUTIONS'',''INWARDS PPN'',''INWARDS XOL'',''MARINE LIABILITY'',
                ''MARINE TRADE'',''POLITICAL VIOLENCE'',''PORTS & TERMINALS'',''PROPERTY'',''CONTINGENCY''
            )
            then stg_product.product_name

            when stg_product.product_name = ''CASUALTY''
            then
                case
                    when ''PROFESSIONAL INDEMNITY'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description) and 
                    (substring(policy.sReference,1,6) <> ''600494'' and substring(policy.sReference,1,6) <> ''011122'' )
                    then ''PROFESSIONAL INDEMNITY''

                    when ''PROFESSIONAL INDEMNITY'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description) and 
                    (substring(policy.sReference,1,6) = ''600494'' or substring(policy.sReference,1,6) = ''011122'' ) 
                    then ''PEN''
                    when stg_coverage.first_coverage_description = ''WARRANTY AND INDEMNITY''
                    then ''WARRANTY & INDEMNITY''
                    when stg_coverage.second_coverage_description = ''WARRANTY AND INDEMNITY''
                    then ''WARRANTY & INDEMNITY''
                    when REPLACE(stg_coverage.first_coverage_description,''&amp;'',''&'') in(''DIRECTORS & OFFICERS'', ''PUBLIC OFFERING OF SECURITIES INSURANCE'')
                    then ''DIRECTORS & OFFICERS''
                    when REPLACE(stg_coverage.second_coverage_description,''&amp;'',''&'') in(''DIRECTORS & OFFICERS'',''PUBLIC OFFERING OF SECURITIES INSURANCE'')
                    then ''DIRECTORS & OFFICERS''

                    when stg_coverage.first_coverage_description in (''MEDICAL MALPRACTICE'',''SURETY BONDS'')
                    then stg_coverage.first_coverage_description
                    when stg_coverage.second_coverage_description in (''MEDICAL MALPRACTICE'',''SURETY BONDS'')
                    then stg_coverage.second_coverage_description

                    when ''LEGAL EXPENSES'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description)
                    AND   sub_class.sSubClassDescr like ''%BTE%'' 
                    then ''LEGAL EXPENSES BTE''

                    when ''LEGAL EXPENSES'' in (stg_coverage.first_coverage_description, stg_coverage.second_coverage_description)
                    AND   sub_class.sSubClassDescr like ''%ATE%'' 
                    then ''LEGAL EXPENSES ATE''

                    else ''CGL''
                end

            when stg_product.product_name = ''DOWNSTREAM ENERGY''
            then
                case
                    when type_of_class.SCLASS in (''POWER GENERATION-CONVENTIONAL'',''POWER GENERATION-RENEWABLES'')
                    then ''POWER & RENEWABLES''
                    else ''OIL & GAS''
                end

            when stg_product.product_name = ''ENGINEERING''
            then
                case
                    when stg_coverage.first_coverage_description in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
                    then ''IDI''
                    when stg_coverage.second_coverage_description in (''INHERENT DEFECTS INSURANCE'',''INSURANCE BACKED GUARANTEE'',''INHERENT DEFECTS INSURANCE - BI'',''INSURANCE BACKED GUARANTEE - BI'')
                    then ''IDI''
                    else ''ENGINEERING''
                end

            when stg_product.product_name = ''FORESTRY''
            then ''PROPERTY''

            when stg_product.product_name = ''MARINE''
            then ''MARINE CARGO''

            when stg_product.product_name = ''UPSTREAM ENERGY''
            then
                case
                    when type_of_class.SCLASS in (''CAR'',''EIO CAR'')
                    then ''UPSTREAM CONSTRUCTION''
                    else ''UPSTREAM ENERGY''
                end

            when stg_product.product_name = ''MR''
            then ''INWARDS XOL''
        end as sub_class_segmentation,

        producer_source_group.broker_major_group_name as producer_source_group_name,
        producer_group.broker_major_group_name as producer_group_name,
        type_of_insurance."STYPEOFINSURANCE" as activity_type_of_insurance,

        -- subdivision: combine Jordan and Bermuda
        -- if subdivision is Jordan then set to Bermuda
        case 
            when stg_subdivision.subdivision = ''JOR'' then ''BER''
            else stg_subdivision.subdivision 
        end as line_subdivision,

        producer_contact.contact_reference as line_producer,
        stg_type_of_profit_centre.producing_office as line_producing_office,
        zone_country_territory.territory as territory_territory,
        zone_country_domicile.territory as domecile_territory,
        stg_coverage.coverage_description as coverage_description,
        stg_coverage.coverage_code as coverage_code,

        case 
            when stg_product.product_name in (
                -- Because neither ''UPSTREAM ENERGY'' nor ''UPSTREAM ENERGY CONSTRUCTION'', can skip above rename logic
                ''INWARDS XOL'',
                ''MR'',
                ''PROPERTY'',
                ''POLITICAL VIOLENCE'',
                ''INWARDS PPN'',
                ''DOWNSTREAM ENERGY''
            )
            then zone_country_territory.territory
            else
                coalesce(zone_country_domicile.territory, zone_country_territory.territory)
        end as line_region,

        case 
            when stg_product.product_name in (
                -- Because neither ''UPSTREAM ENERGY'' nor ''UPSTREAM ENERGY CONSTRUCTION'', can skip above rename logic
                ''INWARDS XOL'',
                ''MR'',
                ''PROPERTY'',
                ''POLITICAL VIOLENCE'',
                ''INWARDS PPN'',
                ''DOWNSTREAM ENERGY''
            )
            then zone_country_territory.country
            else
                coalesce(zone_country_domicile.country, zone_country_territory.country)
        end as region_split,

        coalesce(policy.master_mis_uw_year, policy_line.dtUWYEAR) as mis_uw_year,
        coalesce(policy.master_uw_year, policy.nPeriodFromYear) as uw_year,

        insured_contact.contact_reference as insured,
        reassured_contact.contact_reference as reassured,

        fh_contact.contact_reference as file_handler,
        uw_contact.contact_reference as underwriter,
        rec_uw_contact.contact_reference as recommended_underwriter,
        admin_fh_contact.contact_reference as admin_file_handler,

        activity_operator_id.sFullName as operator_id,
        policy_activity.sNotes as activity_notes,
        nPeriodFromYear,
        ML_cn.contact_reference AS MarketLeader,
        policy_line.dworkinglinepc AS InforceLine,

        type_of_policy_line.sTypeofPolicyLineDescr as New_vs_Renwal,

        policy_line.dSignedLinePc as igi_sgn_line,
        
        -- in the system its stored as a percentage amount(ex : 122.2). divide by 100 to be able to use it in later calculations 
        policy_line.dPlanAdequacy/100 as plan_adequacy,
        policy_line.dTechnicalAdequacy/100 as technical_adequacy,

        policy_line.lModelSequencekey, 


        type_of_pricing.sPricingType as pricing_type,
        type_of_pricing_time.sPricingTime as pricing_time


    from
        policy 
        inner join policy_activity on 
            policy."lpolicykey" = policy_activity."lpolicykey"
        inner join policy_line on
            policy_activity."lPolicyActivityKey" = policy_line."lPolicyActivityKey"
        inner join policy_section on
            policy_activity."lPolicyActivityKey" = policy_section."lPolicyActivityKey"
        left join type_of_activity_source on
            policy_activity."lTypeofActivitySourceKey" = type_of_activity_source."lTypeofActivitySourceKey"
        inner join type_of_class on
            policy_section."lTypeOfClassKey" = type_of_class."LTYPEOFCLASSKEY"
        inner join type_of_placement on
            policy_activity."lTypeOfPlacementKey" = type_of_placement."lTypeOfPlacementKey"
        left join  sub_class on 
            policy_section."lSubClassKey" = sub_class."lSubClassKey"
        inner join stg_product on
            policy_line."lProductKey" = stg_product.product_id
        inner join stg_entity_instance_states on
            policy_activity."lPolicyActivityKey" = stg_entity_instance_states.instance_id
            and stg_entity_instance_states.entity_type = ''Policy Activity''
        inner join stg_entity_instance_states as policy_entity_instance_states on
            policy_activity."lPolicyKey" = policy_entity_instance_states.instance_id
            and policy_entity_instance_states.entity_type = ''Policy''
        left join activity_effective_written_timestamp on
            policy_activity."lPolicyActivityKey" = activity_effective_written_timestamp.activity_id
        inner join type_of_insurance on
            policy_activity."lTypeOfInsuranceKey" = type_of_insurance."LTYPEOFINSURANCEKEY"
        left join type_of_policy_activity on
            policy_activity."lTypeOfPolicyActivityKey" = type_of_policy_activity."lTypeOfPolicyActivityKey"
        inner join stg_subdivision on
            policy_line.lSubDivisionKey = stg_subdivision.subdivision_id
        inner join contact as producer_contact on
            policy_line.lProducerContactKey = producer_contact.contact_id
        inner join stg_type_of_profit_centre on
            policy_line."lTypeOfProfitCentreKey" = stg_type_of_profit_centre.type_of_profit_centre_id
        left join stg_country_territory as zone_country_territory on
            policy_activity."lTerritoryKey" = zone_country_territory.country_id
        left join stg_country_territory as zone_country_domicile on
            policy_activity."lInsuredDomicileCountryKey" = zone_country_domicile.country_id
        left join stg_division on
            policy_line.lDivisionKey = stg_division.division_id
        left join stg_account_period as written_account_period on
            policy_activity."lWrittenAccountPeriodKey" = written_account_period.account_period_id
        left join correct_written_account_period as correct_ap on  
            correct_ap.activity_id = policy_activity.lPolicyActivityKey
        left join stg_coverage on
            policy_section."lPolicySectionKey" = stg_coverage.policy_section_id
        left join classification on
            policy_section."lClassificationKey" = classification."lClassificationKey"
        left join stg_broker_group as producer_source_group on 
            policy_line.lBrokerContactKey = producer_source_group.broker_id
        left join stg_broker_group as producer_group on 
            policy_line.lProducerContactKey = producer_group.broker_id
        left join contact as insured_contact on
            policy_line.lInsuredContactKey = insured_contact.contact_id
        left join contact as reassured_contact on 
            policy_line.lReassuredContactKey = reassured_contact.contact_id
        left join contact as fh_contact on 
            policy_line.lFileHandlerContactKey = fh_contact.contact_id
        left join contact as uw_contact on
            policy_line.lUnderWriterContactKey = uw_contact.contact_id
        left join contact as rec_uw_contact on 
            policy_line.lRecommendedUWContactKey = rec_uw_contact.contact_id
        left join contact as admin_fh_contact on
            policy_line.lAdminFileHandlerContactKey = admin_fh_contact.contact_id
        left join activity_operator_id on 
            policy_activity.lPolicyActivityKey = activity_operator_id.LINSTANCEKEY
        left join Contact ML_cn on
            policy_line.lLeaderContactKey = ML_cn.contact_id
        
        left join type_of_pricing on 
            policy_line.lPricingTypekey = type_of_pricing.lTypeOfPricingKey
        left join type_of_pricing_time on 
            policy_line.lPricingTimekey = type_of_pricing_time.lTypeOfPricingTimeKey
        left join cancellation_reason_hist on
            cancellation_reason_hist.lpolicyactivitykey = policy_activity.lPolicyActivityKey
        left join contra_hist on
            contra_hist.lpolicyactivitykey = policy_activity.lPolicyActivityKey
        
        left join type_of_policy_line on 
            policy_line.lTypeofPolicyLineKey = type_of_policy_line.lTypeofPolicyLineKey    
            
            )final

            
        left join inception_per_account_period on 
            final.policy_id = inception_per_account_period.lpolicyKey and final.written_account_period = inception_per_account_period.accountperiod   
            
),

policy_part_final as(
    select *,
    case 
        when line_region = ''NORTH AMERICA'' and region_split in( ''Canada'', ''BRITISH COLUMBIA'', ''Greenland'') 
        then ''CANADA''
        when  line_region = ''NORTH AMERICA'' and region_split not in( ''Canada'', ''BRITISH COLUMBIA'', ''Greenland'') 
        then ''US''
        else line_region 
    end US_NonUS
    from policy_part
)

select * from policy_part_final
    ');

  CREATE TABLE "dbt_dev"."dmn_policy_part__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_dev].[dmn_policy_part__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_dev"."dmn_policy_part__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_dev"."dmn_policy_part__dbt_tmp_temp_view"
    end



  