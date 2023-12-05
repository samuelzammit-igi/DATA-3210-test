create view "dbt_szammit"."stg_apr__dbt_tmp" as
    

with

apr as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_apr" where _valid_to is null
),

stg_toma as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_type_of_monetary_amount"
),

currency as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_currency" where _valid_to is null
),

stg_account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_account_period"
),

stg_entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_entity_instance_states"
),

entity_matching_journal as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_entity_matching_journal" where _valid_to is null
),

audit_state_transitions as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_audit_state_transitions" where _valid_to is null
),

apr_paid_state_date as (
    select 
        max(DTTRANSITION) apr_paid_date, 
        LINSTANCEKEY 
    from 
        audit_state_transitions 
    where 
        LENTITYKEY = 903 
        and LENTITYSTATEMEMBERKEY = 2347   
    group by LINSTANCEKEY
),

apr_allocation_date as (
    SELECT
        lAccountsPayableReceivableKey,
        MIN(dtEffective) AS dtEffective
    FROM
        (
            SELECT
                EMJ.lRightEntityInstanceKey AS lAccountsPayableReceivableKey ,
                MAX(EMJ.dtEffective) AS dtEffective
            FROM
                entity_matching_journal EMJ
            WHERE
                EMJ.lRightEntityKey = 903
                AND EMJ.lLeftEntityKey IN (903, 690)
            GROUP BY
                EMJ.lRightEntityInstanceKey

            UNION

            SELECT
                EMJ.lLeftEntityInstanceKey AS lAccountsPayableReceivableKey,
                MAX(EMJ.dtEffective) AS dtEffective
            FROM
                entity_matching_journal EMJ
            WHERE
                EMJ.lLeftEntityKey = 903
                AND EMJ.lRightEntityKey IN (903, 690)
            GROUP BY
                EMJ.lLeftEntityInstanceKey
        ) as inner_a
    GROUP BY
        lAccountsPayableReceivableKey
),

stg_apr as (
    select
        apr."SReference"  as  apr_reference,
        apr."lInstanceKey" as apr_instance_id,
        apr."lParentAPRKey" as apr_parent_id,
        apr."lDependentAPRKey"as apr_dependent_id,
        entity_instance_states.entity_state as apr_state,
        apr_entity_instance_states.entity_type_id as apr_entity_type_id,
        apr_entity_instance_states.entity_type as apr_entity_type,
        apr_entity_instance_states.entity_state as apr_entity_state,
        apr."lSourceInstanceKey" as source_instance_id,
        source_entity_instance_states.entity_type_id as source_entity_type_id,
        source_entity_instance_states.entity_type as source_entity_type,
        source_entity_instance_states.entity_state as source_entity_state,
        apr."lParentInstanceKey" as parent_instance_id,
        parent_entity_instance_states.entity_type_id as parent_entity_type_id,
        parent_entity_instance_states.entity_type as parent_entity_type,
        parent_entity_instance_states.entity_state as parent_entity_state,
        apr."dAmount" as orig_amount,
        apr."dBaseAmount" as base_amount,
        apr."dAllocated" as allocated_amount,
        apr."dUnallocated" as unallocated_amount,
        apr."dROE" as apr_roe,
        CAST(APR.dUnallocated/APR.dROE AS DECIMAL(18,2)) AS unallocated_base_amount,
        currency."sCcy" as currency,
        stg_toma.type_of_monetary_amount,
        stg_toma.toma_code,
        apr."dtDue" as due,
        apr."dtEntry" as entry_date,
        stg_account_period.period_name,
        stg_account_period.period_year_period,
        stg_account_period.period_year,
        stg_account_period.period_month,
        stg_account_period.period_start,
        stg_account_period.period_end,

        case
            when cast(apr.dtEntry as date) <= '2019-03-17'
            then apr_allocation_date.dtEffective
            else
                case
                    when entity_instance_states.entity_state = 'Paid'
                    then coalesce(apr_paid_state_date.apr_paid_date, apr_allocation_date.dtEffective)
                end
        end as allocation_date,

        case
            when apr.dAllocated = apr.dAmount
            then 'Allocated'
            else
                case
                    when apr.dAllocated = 0
                    then 'Unallocated'
                    else
                        case
                            when ABS(apr.dAllocated) < ABS(apr.dAmount)
                            then 'Partially Allocated' 
                            else 'Incorrect'
                        end
                end
        end as allocation_status,

        case
            when apr_entity_instance_states.entity_type = 'Policy Line'
            then 'inward_premium'

            when apr_entity_instance_states.entity_type = 'Claim Movement'
            then 'inward_claim'

            when apr.lEntityKey = 742 /* RI Section Broker Security */ and source_entity_instance_states.entity_type = 'Policy Line'
            then 'outward_premium'

            when apr.lEntityKey = 742 /* RI Section Broker Security */ and source_entity_instance_states.entity_type = 'Claim Movement'
            then 'outward_claim_manual'

            when apr.lEntityKey = 742 /* RI Section Broker Security */ and source_entity_instance_states.entity_type = 'APR Message Instalment'
            then 'outward_claim_bureau'

            when apr_entity_instance_states.entity_type = 'RI Claim Movement'
            then 'xol_claim'

            when apr_entity_instance_states.entity_type = 'RI Section'
            then 'xol_premium'

            else apr_entity_instance_states.entity_type
        end as apr_type,

        stg_toma.is_acq_cost,
        stg_toma.is_gross_igi_share,
        is_internal_deductions,
        is_eio,
        is_net_premium_exclude_ipt,

        case when stg_toma.is_acq_cost = 1 then apr.dAmount else 0 end as acq_cost_org,
        case when stg_toma.is_acq_cost = 1 then apr.dBaseAmount else 0 end as acq_cost_base,
        case when stg_toma.is_gross_igi_share = 1 then apr.dAmount else 0 end as gross_igi_share_org,
        case when stg_toma.is_gross_igi_share = 1 then apr.dBaseAmount else 0 end as gross_igi_share_base,
        case when stg_toma.is_internal_deductions = 1 then apr.dAmount else 0 end as internal_deductions_org,
        case when stg_toma.is_internal_deductions = 1 then apr.dBaseAmount else 0 end as internal_deductions_base,
        case when stg_toma.is_eio = 1 then apr.dAmount else 0 end as eio_org,
        case when stg_toma.is_eio = 1 then apr.dBaseAmount else 0 end as eio_base,
        case when stg_toma.is_net_premium_exclude_ipt = 1 then apr.dAmount else 0 end as net_premium_exclude_ipt_org,
        case when stg_toma.is_net_premium_exclude_ipt = 1 then apr.dBaseAmount else 0 end as net_premium_exclude_ipt_base

    from
        apr
        inner join stg_toma on
            apr."lTypeofMonetaryAmountKey" = stg_toma.type_of_monetary_amount_id
        inner join currency on
            apr."lCurrencyKey" = currency."lCurrencyKey"
        inner join stg_account_period on
            apr."lAccountPeriodKey" = stg_account_period.account_period_id
        inner join stg_entity_instance_states as entity_instance_states on
            apr.lAccountsPayableReceivableKey = entity_instance_states.instance_id
            and entity_instance_states.entity_type = 'Accounts Payable Receivable'
        left join stg_entity_instance_states as apr_entity_instance_states on
            apr."lEntityKey" = apr_entity_instance_states.entity_type_id
            and apr."lInstanceKey" = apr_entity_instance_states.instance_id
        left join stg_entity_instance_states as source_entity_instance_states on
            apr."lSourceEntityKey" = source_entity_instance_states.entity_type_id
            and apr."lSourceInstanceKey" = source_entity_instance_states.instance_id
        left join stg_entity_instance_states as parent_entity_instance_states on
            apr."lParentEntityKey" = parent_entity_instance_states.entity_type_id
            and apr."lParentInstanceKey" = parent_entity_instance_states.instance_id
        left join apr_allocation_date on 
            apr.lAccountsPayableReceivableKey = apr_allocation_date.lAccountsPayableReceivableKey
        left join apr_paid_state_date on
            apr.lAccountsPayableReceivableKey = apr_paid_state_date.LINSTANCEKEY

)

select * from stg_apr
