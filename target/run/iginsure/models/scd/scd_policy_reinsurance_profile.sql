create view "dbt_szammit"."scd_policy_reinsurance_profile__dbt_tmp" as
    -- This file is automatically generated


with

Policy_Reinsurance_Profile as (
    select * from "IGI_PROD_DW"."dbo"."PolicyReinsuranceProfile"
),



ordered as (
    select
        lPolicyRIProfileKey,
        lPolicyKey,
        lPolicyActivityKey,
        lPolicyLineKey,
        lPolicySectionKey,
        lPolicyLiabilityKey,
        lPolicyLineLiabilityKey,
        nLevel,
        nLayer,
        nSequence,
        lRIPolicyKey,
        lRIActivityKey,
        lTypeofRIPolicyKey,
        lTypeofRIBasisKey,
        lLayerGrossNetKey,
        lCommissionGrosSNetKey,
        bFixedCession,
        dCessionPC,
        dOverridePC,
        dSurplusCessionPC,
        bAutoProfiled,
        lRISchemeKey,
        lRISectionKey,
        lRISchemeInwardsCoverKey,
        nUniqueCheck,
        bDisabled,
        bCoveragesIncluded,
        doverridePremiumPerc,
        bOverrideDate,
        dOverridePremiumAmt,
        dPremiumAmountShareT,
        dCoverageCessionPC,
        sTreatyNotes,
        dw_loadts,
        row_number() over (
            partition by

                lPolicyRIProfileKey,
                lPolicyKey,
                lPolicyActivityKey,
                lPolicyLineKey,
                lPolicySectionKey,
                lPolicyLiabilityKey,
                lPolicyLineLiabilityKey,
                nLevel,
                nLayer,
                nSequence,
                lRIPolicyKey,
                lRIActivityKey,
                lTypeofRIPolicyKey,
                lTypeofRIBasisKey,
                lLayerGrossNetKey,
                lCommissionGrosSNetKey,
                bFixedCession,
                dCessionPC,
                dOverridePC,
                dSurplusCessionPC,
                bAutoProfiled,
                lRISchemeKey,
                lRISectionKey,
                lRISchemeInwardsCoverKey,
                nUniqueCheck,
                bDisabled,
                bCoveragesIncluded,
                doverridePremiumPerc,
                bOverrideDate,
                dOverridePremiumAmt,
                dPremiumAmountShareT,
                dCoverageCessionPC,
                sTreatyNotes,
                dw_loadts
                
            order by
                dw_loadts
        ) as dw_extract_order
    from
        Policy_Reinsurance_Profile
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
      
        lPolicyRIProfileKey,
        lPolicyKey,
        lPolicyActivityKey,
        lPolicyLineKey,
        lPolicySectionKey,
        lPolicyLiabilityKey,
        lPolicyLineLiabilityKey,
        nLevel,
        nLayer,
        nSequence,
        lRIPolicyKey,
        lRIActivityKey,
        lTypeofRIPolicyKey,
        lTypeofRIBasisKey,
        lLayerGrossNetKey,
        lCommissionGrosSNetKey,
        bFixedCession,
        dCessionPC,
        dOverridePC,
        dSurplusCessionPC,
        bAutoProfiled,
        lRISchemeKey,
        lRISectionKey,
        lRISchemeInwardsCoverKey,
        nUniqueCheck,
        bDisabled,
        bCoveragesIncluded,
        doverridePremiumPerc,
        bOverrideDate,
        dOverridePremiumAmt,
        dPremiumAmountShareT,
        dCoverageCessionPC,
        sTreatyNotes,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lPolicyRIProfileKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
