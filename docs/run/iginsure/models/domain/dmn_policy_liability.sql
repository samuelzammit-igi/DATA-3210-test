create view "dbt_szammit"."dmn_policy_liability__dbt_tmp" as
    

with

policy_liability as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_policy_liability" where _valid_to is null
),

used_fields as (
    select
        lPolicyActivityKey as activity_id,
        lPolicySectionKey as section_id,
        dLiabilityPremiumROE,
        case
            when dVersionGrossThisTime100 is null then 0 else dVersionGrossThisTime100
        end as dVersionGrossThisTime100,
        case
            when dVersionGrossThisTimeShareTT is null then 0 else dVersionGrossThisTimeShareTT
        end as dVersionGrossThisTimeShareTT,
        case
            when dEventLimit100 is null then 0 else dEventLimit100
        end as dEventLimit100,
        dLimitCurrencyROE,
        case
            when dExcessOf is null then 0 else dExcessOf
        end as dExcessOf,
        case
            when dDeductible is null then 0 else dDeductible
        end as dDeductible,
        case
            when dTSI is null then 0 else dTSI
        end as dTSI
    from
        policy_liability
),

calculations as (
    select
        activity_id,
        section_id,
        case
            when dLiabilityPremiumROE is null or dLiabilityPremiumROE = 0 then 0
            else (dVersionGrossThisTime100 / dLiabilityPremiumROE)
        end as GrossWritten100,
        case
            when dLiabilityPremiumROE is null or dLiabilityPremiumROE = 0 then 0
            else (dVersionGrossThisTimeShareTT / dLiabilityPremiumROE)
        end as GrossWrittenShare,
        case
            when dLimitCurrencyROE is null or dLimitCurrencyROE = 0 then 0
            else (dEventLimit100 / dLimitCurrencyROE)
        end as EventLimit,
        case
            when dLimitCurrencyROE is null or dLimitCurrencyROE = 0 then 0
            else (dExcessOf / dLimitCurrencyROE)
        end as XSPoint,
        case
            when dLimitCurrencyROE is null or dLimitCurrencyROE = 0 then 0
            else (dDeductible / dLimitCurrencyROE)
        end as Deductible,
        case
            when dLimitCurrencyROE is null or dLimitCurrencyROE = 0 then 0
            else (dTSI / dLimitCurrencyROE)
        end as TotalSumInsured
    from
        used_fields
)

select * from calculations
