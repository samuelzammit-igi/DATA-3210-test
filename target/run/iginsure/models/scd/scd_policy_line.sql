create view "dbt_dev"."scd_policy_line__dbt_tmp" as
    -- This file is automatically generated


with

policy_line as (
    select * from "IGI_PROD_DW"."dbo"."PolicyLine"
),

ordered as (
    select
        lPolicyLineKey,
        lPolicyActivityKey,
        lPolicyKey,
        lPolicyFolderKey,
        lMarketSourceKey,
        lDivisionKey,
        lSubDivisionKey,
        lMarketDivisionKey,
        lMarketSubDivisionKey,
        lMarketAreaKey,
        sPolicyLineReference,
        dtDateWritten,
        dParticipationPc,
        dWrittenLinePc,
        dWrittenLineLimitShare,
        dWrittenLineLimit100,
        dEstimatedSigningPc,
        dBrokersOrderPc,
        dActualEstimatedLinePc,
        dtDateSigned,
        dSignedLinePc,
        dSignedOrderPc,
        dActualSignedLinePc,
        dSignedDownPc,
        dVersionActualSignedLinePc,
        dWorkingLinePc,
        dPreviousActualSignedLinePc,
        dOverrideSignedLinePC,
        dOverrideSignedOrderPC,
        lWrittenAccountPeriodKey,
        lSignedAccountPeriodKey,
        lTypeOfEstimatedLineKey,
        bSignDownPossible,
        bSignedByDelink,
        bWrittenLineToStand,
        lTypeofLeaderKey,
        lLeaderContactKey,
        dLeadUWLinePC,
        bCoinsuranceExists,
        sLeadReference,
        bCoinsurancePremiumAccounting,
        bCoinsuranceClaimAccounting,
        dCoinsuranceTotalPC,
        lTypeOfMarketSourceKey,
        bSubscriptionMarket,
        bLeader,
        lTypeOfBillingKey,
        bLeaderManagesAdditions,
        lFrontingCompanyContactKey,
        sFrontingOfficeRef,
        dFrontingOrderPC,
        lSummaryCcyKey,
        dSummaryCcyROE,
        nCountSoleLeader,
        lBoundAccountPeriodKey,
        lProductCarrierKey,
        lProductCarrierStateKey,
        lCarrierContactKey,
        lProductKey,
        lProductVersionCarrierKey,
        lProductVersionTemplateKey,
        lAddressCountryKey,
        lAddressStateKey,
        bKeyAddrStateUsedInProdCarrier,
        bDocumentCarrier,
        dtCancel,
        bProductRuleResult,
        bProductHardRuleResult,
        nCountDeclinedReason,
        bAuthorisationResult,
        bNewLineThisActivity,
        bNewLineThisEndorsement,
        dtPeriodFrom,
        bUndoQuoteNTUReasonEntered,
        bQuoteNTUReasonEntered,
        lUnderWriterContactKey,
        lUnderwritingAssistContactKey,
        lPolicyLineChainKey,
        bRunSigningProcess,
        bMsgProcessed,
        lTypeOfDivisionKey,
        nMGACounter,
        bPeerReviewRequired,
        nNumberofReviewers,
        nNumberofChecks,
        nNumberofCompleteChecks,
        lFirstPeerReviewApprovedBy,
        lSecondPeerReviewApprovedBy,
        bMasterPolicyUpdated,
        nUnassignedUnderwriters,
        lWorkingAccountPeriodKey,
        binvoicecreated,
        nSigningOverridden,
        dDelinkedSignedLinePC,
        dDelinkedBrokerOrderPC,
        bRunDelinkedSigningProcessFlag,
        nSectionsCoveredbyLine,
        lTypeOfProfitCentreKey,
        lAgreementActivityKey,
        dCodeSplitsTotalpc,
        bLimitWithinAgreement,
        bPremiumWithinAgreement,
        bCountryUsedInProductTemplate,
        sPolicyLineReferenceOverride,
        bReferenceUnique,
        bEffectiveDateOutOfRange,
        bVoided,
        sDivisionText,
        sSubDivisionText,
        sCombinedDivisionText,
        lLatestUWAssistContactKey,
        lLatestUWAssistDefaultDivisionKey,
        lLatestUWAssistDefaultSubDivisionKey,
        lPresentationCurrencyKey,
        bCloseClaimAFP,
        bCannotPayClaimsLTR,
        bAllowOverridePolicyReference,
        bImmediateGeneratePolicyReference,
        bAllowOverridePolicyReferenceFlag,
        bFirstPolicyLineRow,
        lBrokerContactKey,
        lProducerContactKey,
        lInsuredContactKey,
        lReassuredContactKey,
        bFrontingFeeCheck,
        bHasBeenSigned,
        lSigningPolicyLineChainKey,
        dPMLLimit100,
        dPMLLimitShare,
        dFACLimit100,
        dPMLLimitNetofFac100,
        dPMLLimitNetofFacShare,
        dMaxPMLLimitShare,
        dMaxEventLimitShare,
        bLimitChecked,
        bLimitOverride,
        dFACLimitShare,
        dEventLimit100,
        dEventLimitShare,
        bCreateFac,
        lAutoCreatedFacRIFolderKey,
        bFacCreated,
        bLimitAllowed,
        dPolicyLineLimitCurrencyKey,
        dPolicyLineLimitROE,
        dMaxPMLLimit,
        dMaxEventLimit,
        lIGIAgreementParty,
        bNRMLossEffected,
        dtTechnicalWrittenSigned,
        lOriginalBrokerContactKey,
        lSurplusBrokerContactKey,
        lFileHandlerContactKey,
        lAdminFileHandlerContactKey,
        lRecommendedUWContactKey,
        lzoneCountryKey,
        lterritorykey,
        lSolvencyCountryKey,
        lSlipLeaderContactKey,
        nNoOfReinstatements,
        lReinstatementCCYKey,
        dReinstatementAggLimit,
        lTypeofBaseofReinstatementkey,
        lTypeofStampCodeKey,
        lTypeofCyberRiskKey,
        dTSI,
        lTypeOfERPNoOfYears,
        lTypeofProjectKey,
        lTypeofNetRateMovementkey,
        dNetRateMovement,
        bAuthorisedProducer,
        bAuthorisedInsured,
        bAuthorisedReassured,
        bAuthorisedBroker,
        dPremiumAmountShareT,
        bRIAutoProfilingRequired,
        nNextClaimNo,
        lUnapprovedBrokerKey,
        lDomicileCountryKey,
        lTypeofSanctionsClauseKey,
        bPPL,
        lTypeofPolicyLineKey,
        dtUWyear,
        sPolicyLineBKReference,
        lTypeofTreatyStatusKey,
        lProductVersionTemplateCloneKey,
        bDoNotGenerateRI,
        lMasterPolicyActivityKey,
        dAuthorizedPrem,
        bPremiumAuthorized,
        bRequiresKYCDivision,
        bisSignlineApproved,
        lLinkRenewalPolicyKey,
        bLinkRenewalRule,
        lTypeOfPolicyActivityKey,
        lPricingTypekey,
        lPricingTimekey,
        lModelSequencekey,
        dPlanAdequacy,
        dTechnicalAdequacy,
        dGrossPremium,
        dGrossNetPremium,
        dExposurePremium,
        dNonProgrammeDeductable,
        dTermsConditions,
        dGrossRateMovementBasic,
        dNetRateMovementBasic,
        dGrossRateMovementFull,
        dNetRateMovementFull,
        dw_loadts,
        row_number() over (
            partition by
                lPolicyLineKey,
                lPolicyActivityKey,
                lPolicyKey,
                lPolicyFolderKey,
                lMarketSourceKey,
                lDivisionKey,
                lSubDivisionKey,
                lMarketDivisionKey,
                lMarketSubDivisionKey,
                lMarketAreaKey,
                sPolicyLineReference,
                dtDateWritten,
                dParticipationPc,
                dWrittenLinePc,
                dWrittenLineLimitShare,
                dWrittenLineLimit100,
                dEstimatedSigningPc,
                dBrokersOrderPc,
                dActualEstimatedLinePc,
                dtDateSigned,
                dSignedLinePc,
                dSignedOrderPc,
                dActualSignedLinePc,
                dSignedDownPc,
                dVersionActualSignedLinePc,
                dWorkingLinePc,
                dPreviousActualSignedLinePc,
                dOverrideSignedLinePC,
                dOverrideSignedOrderPC,
                lWrittenAccountPeriodKey,
                lSignedAccountPeriodKey,
                lTypeOfEstimatedLineKey,
                bSignDownPossible,
                bSignedByDelink,
                bWrittenLineToStand,
                lTypeofLeaderKey,
                lLeaderContactKey,
                dLeadUWLinePC,
                bCoinsuranceExists,
                sLeadReference,
                bCoinsurancePremiumAccounting,
                bCoinsuranceClaimAccounting,
                dCoinsuranceTotalPC,
                lTypeOfMarketSourceKey,
                bSubscriptionMarket,
                bLeader,
                lTypeOfBillingKey,
                bLeaderManagesAdditions,
                lFrontingCompanyContactKey,
                sFrontingOfficeRef,
                dFrontingOrderPC,
                lSummaryCcyKey,
                dSummaryCcyROE,
                nCountSoleLeader,
                lBoundAccountPeriodKey,
                lProductCarrierKey,
                lProductCarrierStateKey,
                lCarrierContactKey,
                lProductKey,
                lProductVersionCarrierKey,
                lProductVersionTemplateKey,
                lAddressCountryKey,
                lAddressStateKey,
                bKeyAddrStateUsedInProdCarrier,
                bDocumentCarrier,
                dtCancel,
                bProductRuleResult,
                bProductHardRuleResult,
                nCountDeclinedReason,
                bAuthorisationResult,
                bNewLineThisActivity,
                bNewLineThisEndorsement,
                dtPeriodFrom,
                bUndoQuoteNTUReasonEntered,
                bQuoteNTUReasonEntered,
                lUnderWriterContactKey,
                lUnderwritingAssistContactKey,
                lPolicyLineChainKey,
                bRunSigningProcess,
                bMsgProcessed,
                lTypeOfDivisionKey,
                nMGACounter,
                bPeerReviewRequired,
                nNumberofReviewers,
                nNumberofChecks,
                nNumberofCompleteChecks,
                lFirstPeerReviewApprovedBy,
                lSecondPeerReviewApprovedBy,
                bMasterPolicyUpdated,
                nUnassignedUnderwriters,
                lWorkingAccountPeriodKey,
                binvoicecreated,
                nSigningOverridden,
                dDelinkedSignedLinePC,
                dDelinkedBrokerOrderPC,
                bRunDelinkedSigningProcessFlag,
                nSectionsCoveredbyLine,
                lTypeOfProfitCentreKey,
                lAgreementActivityKey,
                dCodeSplitsTotalpc,
                bLimitWithinAgreement,
                bPremiumWithinAgreement,
                bCountryUsedInProductTemplate,
                sPolicyLineReferenceOverride,
                bReferenceUnique,
                bEffectiveDateOutOfRange,
                bVoided,
                sDivisionText,
                sSubDivisionText,
                sCombinedDivisionText,
                lLatestUWAssistContactKey,
                lLatestUWAssistDefaultDivisionKey,
                lLatestUWAssistDefaultSubDivisionKey,
                lPresentationCurrencyKey,
                bCloseClaimAFP,
                bCannotPayClaimsLTR,
                bAllowOverridePolicyReference,
                bImmediateGeneratePolicyReference,
                bAllowOverridePolicyReferenceFlag,
                bFirstPolicyLineRow,
                lBrokerContactKey,
                lProducerContactKey,
                lInsuredContactKey,
                lReassuredContactKey,
                bFrontingFeeCheck,
                bHasBeenSigned,
                lSigningPolicyLineChainKey,
                dPMLLimit100,
                dPMLLimitShare,
                dFACLimit100,
                dPMLLimitNetofFac100,
                dPMLLimitNetofFacShare,
                dMaxPMLLimitShare,
                dMaxEventLimitShare,
                bLimitChecked,
                bLimitOverride,
                dFACLimitShare,
                dEventLimit100,
                dEventLimitShare,
                bCreateFac,
                lAutoCreatedFacRIFolderKey,
                bFacCreated,
                bLimitAllowed,
                dPolicyLineLimitCurrencyKey,
                dPolicyLineLimitROE,
                dMaxPMLLimit,
                dMaxEventLimit,
                lIGIAgreementParty,
                bNRMLossEffected,
                dtTechnicalWrittenSigned,
                lOriginalBrokerContactKey,
                lSurplusBrokerContactKey,
                lFileHandlerContactKey,
                lAdminFileHandlerContactKey,
                lRecommendedUWContactKey,
                lzoneCountryKey,
                lterritorykey,
                lSolvencyCountryKey,
                lSlipLeaderContactKey,
                nNoOfReinstatements,
                lReinstatementCCYKey,
                dReinstatementAggLimit,
                lTypeofBaseofReinstatementkey,
                lTypeofStampCodeKey,
                lTypeofCyberRiskKey,
                dTSI,
                lTypeOfERPNoOfYears,
                lTypeofProjectKey,
                lTypeofNetRateMovementkey,
                dNetRateMovement,
                bAuthorisedProducer,
                bAuthorisedInsured,
                bAuthorisedReassured,
                bAuthorisedBroker,
                dPremiumAmountShareT,
                bRIAutoProfilingRequired,
                nNextClaimNo,
                lUnapprovedBrokerKey,
                lDomicileCountryKey,
                lTypeofSanctionsClauseKey,
                bPPL,
                lTypeofPolicyLineKey,
                dtUWyear,
                sPolicyLineBKReference,
                lTypeofTreatyStatusKey,
                lProductVersionTemplateCloneKey,
                bDoNotGenerateRI,
                lMasterPolicyActivityKey,
                dAuthorizedPrem,
                bPremiumAuthorized,
                bRequiresKYCDivision,
                bisSignlineApproved,
                lLinkRenewalPolicyKey,
                bLinkRenewalRule,
                lTypeOfPolicyActivityKey,
                lPricingTypekey,
                lPricingTimekey,
                lModelSequencekey,
                dPlanAdequacy,
                dTechnicalAdequacy,
                dGrossPremium,
                dGrossNetPremium,
                dExposurePremium,
                dNonProgrammeDeductable,
                dTermsConditions,
                dGrossRateMovementBasic,
                dNetRateMovementBasic,
                dGrossRateMovementFull,
                dNetRateMovementFull
            order by
                dw_loadts
        ) as dw_extract_order
    from
        policy_line
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lPolicyLineKey,
        lPolicyActivityKey,
        lPolicyKey,
        lPolicyFolderKey,
        lMarketSourceKey,
        lDivisionKey,
        lSubDivisionKey,
        lMarketDivisionKey,
        lMarketSubDivisionKey,
        lMarketAreaKey,
        sPolicyLineReference,
        dtDateWritten,
        dParticipationPc,
        dWrittenLinePc,
        dWrittenLineLimitShare,
        dWrittenLineLimit100,
        dEstimatedSigningPc,
        dBrokersOrderPc,
        dActualEstimatedLinePc,
        dtDateSigned,
        dSignedLinePc,
        dSignedOrderPc,
        dActualSignedLinePc,
        dSignedDownPc,
        dVersionActualSignedLinePc,
        dWorkingLinePc,
        dPreviousActualSignedLinePc,
        dOverrideSignedLinePC,
        dOverrideSignedOrderPC,
        lWrittenAccountPeriodKey,
        lSignedAccountPeriodKey,
        lTypeOfEstimatedLineKey,
        bSignDownPossible,
        bSignedByDelink,
        bWrittenLineToStand,
        lTypeofLeaderKey,
        lLeaderContactKey,
        dLeadUWLinePC,
        bCoinsuranceExists,
        sLeadReference,
        bCoinsurancePremiumAccounting,
        bCoinsuranceClaimAccounting,
        dCoinsuranceTotalPC,
        lTypeOfMarketSourceKey,
        bSubscriptionMarket,
        bLeader,
        lTypeOfBillingKey,
        bLeaderManagesAdditions,
        lFrontingCompanyContactKey,
        sFrontingOfficeRef,
        dFrontingOrderPC,
        lSummaryCcyKey,
        dSummaryCcyROE,
        nCountSoleLeader,
        lBoundAccountPeriodKey,
        lProductCarrierKey,
        lProductCarrierStateKey,
        lCarrierContactKey,
        lProductKey,
        lProductVersionCarrierKey,
        lProductVersionTemplateKey,
        lAddressCountryKey,
        lAddressStateKey,
        bKeyAddrStateUsedInProdCarrier,
        bDocumentCarrier,
        dtCancel,
        bProductRuleResult,
        bProductHardRuleResult,
        nCountDeclinedReason,
        bAuthorisationResult,
        bNewLineThisActivity,
        bNewLineThisEndorsement,
        dtPeriodFrom,
        bUndoQuoteNTUReasonEntered,
        bQuoteNTUReasonEntered,
        lUnderWriterContactKey,
        lUnderwritingAssistContactKey,
        lPolicyLineChainKey,
        bRunSigningProcess,
        bMsgProcessed,
        lTypeOfDivisionKey,
        nMGACounter,
        bPeerReviewRequired,
        nNumberofReviewers,
        nNumberofChecks,
        nNumberofCompleteChecks,
        lFirstPeerReviewApprovedBy,
        lSecondPeerReviewApprovedBy,
        bMasterPolicyUpdated,
        nUnassignedUnderwriters,
        lWorkingAccountPeriodKey,
        binvoicecreated,
        nSigningOverridden,
        dDelinkedSignedLinePC,
        dDelinkedBrokerOrderPC,
        bRunDelinkedSigningProcessFlag,
        nSectionsCoveredbyLine,
        lTypeOfProfitCentreKey,
        lAgreementActivityKey,
        dCodeSplitsTotalpc,
        bLimitWithinAgreement,
        bPremiumWithinAgreement,
        bCountryUsedInProductTemplate,
        sPolicyLineReferenceOverride,
        bReferenceUnique,
        bEffectiveDateOutOfRange,
        bVoided,
        sDivisionText,
        sSubDivisionText,
        sCombinedDivisionText,
        lLatestUWAssistContactKey,
        lLatestUWAssistDefaultDivisionKey,
        lLatestUWAssistDefaultSubDivisionKey,
        lPresentationCurrencyKey,
        bCloseClaimAFP,
        bCannotPayClaimsLTR,
        bAllowOverridePolicyReference,
        bImmediateGeneratePolicyReference,
        bAllowOverridePolicyReferenceFlag,
        bFirstPolicyLineRow,
        lBrokerContactKey,
        lProducerContactKey,
        lInsuredContactKey,
        lReassuredContactKey,
        bFrontingFeeCheck,
        bHasBeenSigned,
        lSigningPolicyLineChainKey,
        dPMLLimit100,
        dPMLLimitShare,
        dFACLimit100,
        dPMLLimitNetofFac100,
        dPMLLimitNetofFacShare,
        dMaxPMLLimitShare,
        dMaxEventLimitShare,
        bLimitChecked,
        bLimitOverride,
        dFACLimitShare,
        dEventLimit100,
        dEventLimitShare,
        bCreateFac,
        lAutoCreatedFacRIFolderKey,
        bFacCreated,
        bLimitAllowed,
        dPolicyLineLimitCurrencyKey,
        dPolicyLineLimitROE,
        dMaxPMLLimit,
        dMaxEventLimit,
        lIGIAgreementParty,
        bNRMLossEffected,
        dtTechnicalWrittenSigned,
        lOriginalBrokerContactKey,
        lSurplusBrokerContactKey,
        lFileHandlerContactKey,
        lAdminFileHandlerContactKey,
        lRecommendedUWContactKey,
        lzoneCountryKey,
        lterritorykey,
        lSolvencyCountryKey,
        lSlipLeaderContactKey,
        nNoOfReinstatements,
        lReinstatementCCYKey,
        dReinstatementAggLimit,
        lTypeofBaseofReinstatementkey,
        lTypeofStampCodeKey,
        lTypeofCyberRiskKey,
        dTSI,
        lTypeOfERPNoOfYears,
        lTypeofProjectKey,
        lTypeofNetRateMovementkey,
        dNetRateMovement,
        bAuthorisedProducer,
        bAuthorisedInsured,
        bAuthorisedReassured,
        bAuthorisedBroker,
        dPremiumAmountShareT,
        bRIAutoProfilingRequired,
        nNextClaimNo,
        lUnapprovedBrokerKey,
        lDomicileCountryKey,
        lTypeofSanctionsClauseKey,
        bPPL,
        lTypeofPolicyLineKey,
        dtUWyear,
        sPolicyLineBKReference,
        lTypeofTreatyStatusKey,
        lProductVersionTemplateCloneKey,
        bDoNotGenerateRI,
        lMasterPolicyActivityKey,
        dAuthorizedPrem,
        bPremiumAuthorized,
        bRequiresKYCDivision,
        bisSignlineApproved,
        lLinkRenewalPolicyKey,
        bLinkRenewalRule,
        lTypeOfPolicyActivityKey,
        lPricingTypekey,
        lPricingTimekey,
        lModelSequencekey,
        dPlanAdequacy,
        dTechnicalAdequacy,
        dGrossPremium,
        dGrossNetPremium,
        dExposurePremium,
        dNonProgrammeDeductable,
        dTermsConditions,
        dGrossRateMovementBasic,
        dNetRateMovementBasic,
        dGrossRateMovementFull,
        dNetRateMovementFull,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lPolicyLineKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
