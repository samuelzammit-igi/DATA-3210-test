create view "dbt_szammit"."scd_claim__dbt_tmp" as
    -- This file is automatically generated

with

claim as (
    select * from "IGI_PROD_DW"."dbo"."Claim"
),

ordered as (
    select
        lClaimKey,
        bBordereauClaim,
        bBureauClaim,
        bClaimOutsideTerm,
        bECFClaim,
        bfaconpolicy,
        bInlitigation,
        bLeader,
        bLOC,
        bLossDateTBA,
        bManualBlockIndicator,
        bManuallyCorrected,
        bNoticeToReinsurer,
        bOutOfScope,
        bPremiumFullyEarned,
        bRIApplies,
        bSalvage,
        bSigned,
        bSubrogation,
        bsurplustreatyonpolicy,
        BTOTALLOSS,
        bWordingDeficient,
        DCURRENTEXPENSES,
        dCurrentExpensesReserve,
        dCurrentFeePaid,
        dCurrentFeeReserve,
        DCURRENTINDEMNITY,
        dCurrentIndemnityReserve,
        dCurrentTotalPaid,
        dCurrentTotalReserve,
        dInitialExpensesReserve,
        dInitialFeeReserve,
        dInitialIndemnityReserve,
        dInitialTotalReserve,
        dLOCCurrentBalance,
        dLOCInitialBalance,
        dLOCTotalDrawings,
        dMonitorCurrentPaidTotal,
        dPolicyActivityROE,
        dRateOfExchange,
        dSectionLinePC,
        dSubrogationAmount,
        dSubrogationSettlementAmount,
        dtClaim1stReported,
        dtClaimAttached,
        dtClaimMadeFrom,
        dtClaimMadeTo,
        dtClosed,
        dtLossFrom,
        dtDiscovery,
        dtLastReviewed,
        dtLossTo,
        DTREPORTED,
        dtSubrogationDateCompleted,
        dtSubrogationDateStarted,
        lAddressLookupKey,
        lAgreementActivityKey,
        lBordereauLiabilityKey,
        lAgreementBordereauLocationKey,
        lAgreementBordereauPolicyKey,
        lBordereauTransClmCanAttachKey,
        lBordereauTransactionKey,
        lAgreementFolderKey,
        lAgreementInsuredKey,
        lAgreementYearKey,
        lBlobBackgroundKey,
        lBlobCoverageKey,
        lBlobDamagesKey,
        lBlobLiabilityKey,
        lBlobPlanOfActionKey,
        lBlobReserveCommentaryKey,
        lBrokerContactLiaisonKey,
        lBrokerKey,
        lBureauLeaderContactKey,
        lCatCodeKey,
        lClaimantKey,
        lClaimsCauseCategoryKey,
        LCLAIMSCAUSETYPEKEY,
        lClaimsLeadType,
        lCurrencyKey,
        lDivisionKey,
        lEventKey,
        lExternalAdjusterContactKey,
        lExternalAttorneyContactKey,
        lInsuredKey,
        lInternalAdjusterContactKey,
        lInternalAttorneyContactKey,
        lLossLocationCityKey,
        lLossLocationCountryKey,
        lLossLocationCountryRegionKey,
        lLossLocationPostCodeKey,
        lLossLocationStateKey,
        lPlaintiffAttorneyContactKey,
        lPolicyActivityCurrencyKey,
        lPolicyActivityKey,
        lPolicyBrokerKey,
        lPolicyBrokerLiaisonKey,
        lPolicyDivisionKey,
        lPolicyFolderKey,
        lPolicyInsuredKey,
        lPolicyKey,
        lPolicySectionKey,
        lPolicySubDivisionKey,
        lResponsiblePartyKey,
        lSlipLeaderContactKey,
        lSubDivisionKey,
        lSubEventKey,
        lTypeOfCauseKey,
        lTypeOfClaimClosedKey,
        lTypeOfLossKey,
        lTypeOfLossLocationKey,
        lTypeOfMarketSourceKey,
        lTypeOfNAICCodeKey,
        lTypeOfNoticeToReinsurerKey,
        lTypeOfPolicyActivityKey,
        lUnderwriterContactKey,
        lVesselKey,
        nAccidentYear,
        nInvalidClassCounter,
        nYearOfAccount,
        sBrokerReference1,
        sBrokerReference2,
        sClaimDescription,
        sClaimReference,
        sClassUCR,
        sExternalAdjusterReference,
        sExternalAttorneyReference,
        sFireIncidentReference,
        sLocationName,
        sLossCulprit,
        sLossLocationAddress1,
        sLossLocationAddress2,
        sLossLocationCity,
        sLossLocationPostCode,
        sMarketReference,
        sOrigClaimReference,
        sPCSCode,
        sPlaintiffAttorneyReference,
        sPoliceIncidentReference,
        sPolicyRef,
        sTR,
        sUCR,
        sUMR,
        bPolicyPremiumPaid,
        bManuallyCorrectedComplete,
        dPolicySectionLinePC,
        dtDenied,
        sLossLocationLat,
        sLossLocationLong,
        sLossDateNotes,
        lPaperKey,
        sInsuredPhone,
        sInsuredEmail,
        sBrokerPhone,
        sBrokerEmail,
        sBrokerContactPhone,
        sBrokerContactEmail,
        sExternalAdjusterPhone,
        sExternalAdjusterEmail,
        nProofNotPresentedCount,
        lCombinedPolicyFolderKey,
        lCombinedPolicyKey,
        lCombinedPolicySectionKey,
        lCombinedPolicyActivityKey,
        lEntityKey,
        lInstanceKey,
        lAgreementSectionKey,
        dAgreementSectionLinePC,
        lCurrencyKeyAgrmt,
        dROEAgrmt,
        bCoinsuranceClaimAccounting,
        lPolicyLineKey,
        bCoinsuranceExists,
        bOverRideAttachmentRule,
        sVoyageFrom,
        sVoyageTo,
        sBackground,
        sCoverage,
        sDamages,
        sLiability,
        sPlanOfAction,
        sReserveCommentary,
        lPolicyLineChainKey,
        dMinSignedLine,
        dMaxSignedLine,
        bPolicyLeader,
        lPolicyTypeOfLeaderKey,
        nPolicyYearOfAccount,
        bBinderPolicy,
        lTypeOfClaimSeverityKey,
        lClaimHandlerKey,
        bCloseClaimAFP,
        bCannotPayClaimsLTR,
        dtReopenedDate,
        dtCreatedDate,
        dtClaimAmountAgreedDate,
        lClaimStatusKey,
        dtWithdrawnDate,
        bPolicyCoverageMandatory,
        bPolicySectionMandatory,
        lvPolicyMandatoryFlagsKey,
        lCombinedPolicyListKey,
        bDOLOverride,
        bAuthorisedCatCode,
        dtNextReviewedSixMonths,
        dtNextReviewed,
        bDOLEntered,
        lTypeOfRAGKey,
        dClaimPotential,
        lReinstatementCurrencyKey,
        dReinstatementROE,
        dReinstatementTotalReserve,
        dReinstatementTotalPaidToDate,
        dReinstatementTotalDue,
        dReinstatementTotalReserveReport,
        dReinstatementTotalPaidToDateReport,
        dReinstatementTotalDueReport,
        bManualDuplicateExists,
        lClaimGroupKey,
        sManualDuplicateUCRCheck,
        dtReopened,
        lTypeofEventType,
        dtBrokerClaimReceived,
        dtPolicySignedDate,
        bTreaty,
        bDeligatedAuthority,
        lSCAPKey,
        bAuthorisedEvent,
        bFullFollow,
        bNoHardCopyFile,
        sClaimant,
        lClaimPotentialCcyKey,
        bDivisionChange,
        dw_loadts,
        row_number() over (
            partition by
                lClaimKey,
                bBordereauClaim,
                bBureauClaim,
                bClaimOutsideTerm,
                bECFClaim,
                bfaconpolicy,
                bInlitigation,
                bLeader,
                bLOC,
                bLossDateTBA,
                bManualBlockIndicator,
                bManuallyCorrected,
                bNoticeToReinsurer,
                bOutOfScope,
                bPremiumFullyEarned,
                bRIApplies,
                bSalvage,
                bSigned,
                bSubrogation,
                bsurplustreatyonpolicy,
                BTOTALLOSS,
                bWordingDeficient,
                DCURRENTEXPENSES,
                dCurrentExpensesReserve,
                dCurrentFeePaid,
                dCurrentFeeReserve,
                DCURRENTINDEMNITY,
                dCurrentIndemnityReserve,
                dCurrentTotalPaid,
                dCurrentTotalReserve,
                dInitialExpensesReserve,
                dInitialFeeReserve,
                dInitialIndemnityReserve,
                dInitialTotalReserve,
                dLOCCurrentBalance,
                dLOCInitialBalance,
                dLOCTotalDrawings,
                dMonitorCurrentPaidTotal,
                dPolicyActivityROE,
                dRateOfExchange,
                dSectionLinePC,
                dSubrogationAmount,
                dSubrogationSettlementAmount,
                dtClaim1stReported,
                dtClaimAttached,
                dtClaimMadeFrom,
                dtClaimMadeTo,
                dtClosed,
                dtLossFrom,
                dtDiscovery,
                dtLastReviewed,
                dtLossTo,
                DTREPORTED,
                dtSubrogationDateCompleted,
                dtSubrogationDateStarted,
                lAddressLookupKey,
                lAgreementActivityKey,
                lBordereauLiabilityKey,
                lAgreementBordereauLocationKey,
                lAgreementBordereauPolicyKey,
                lBordereauTransClmCanAttachKey,
                lBordereauTransactionKey,
                lAgreementFolderKey,
                lAgreementInsuredKey,
                lAgreementYearKey,
                lBlobBackgroundKey,
                lBlobCoverageKey,
                lBlobDamagesKey,
                lBlobLiabilityKey,
                lBlobPlanOfActionKey,
                lBlobReserveCommentaryKey,
                lBrokerContactLiaisonKey,
                lBrokerKey,
                lBureauLeaderContactKey,
                lCatCodeKey,
                lClaimantKey,
                lClaimsCauseCategoryKey,
                LCLAIMSCAUSETYPEKEY,
                lClaimsLeadType,
                lCurrencyKey,
                lDivisionKey,
                lEventKey,
                lExternalAdjusterContactKey,
                lExternalAttorneyContactKey,
                lInsuredKey,
                lInternalAdjusterContactKey,
                lInternalAttorneyContactKey,
                lLossLocationCityKey,
                lLossLocationCountryKey,
                lLossLocationCountryRegionKey,
                lLossLocationPostCodeKey,
                lLossLocationStateKey,
                lPlaintiffAttorneyContactKey,
                lPolicyActivityCurrencyKey,
                lPolicyActivityKey,
                lPolicyBrokerKey,
                lPolicyBrokerLiaisonKey,
                lPolicyDivisionKey,
                lPolicyFolderKey,
                lPolicyInsuredKey,
                lPolicyKey,
                lPolicySectionKey,
                lPolicySubDivisionKey,
                lResponsiblePartyKey,
                lSlipLeaderContactKey,
                lSubDivisionKey,
                lSubEventKey,
                lTypeOfCauseKey,
                lTypeOfClaimClosedKey,
                lTypeOfLossKey,
                lTypeOfLossLocationKey,
                lTypeOfMarketSourceKey,
                lTypeOfNAICCodeKey,
                lTypeOfNoticeToReinsurerKey,
                lTypeOfPolicyActivityKey,
                lUnderwriterContactKey,
                lVesselKey,
                nAccidentYear,
                nInvalidClassCounter,
                nYearOfAccount,
                sBrokerReference1,
                sBrokerReference2,
                sClaimDescription,
                sClaimReference,
                sClassUCR,
                sExternalAdjusterReference,
                sExternalAttorneyReference,
                sFireIncidentReference,
                sLocationName,
                sLossCulprit,
                sLossLocationAddress1,
                sLossLocationAddress2,
                sLossLocationCity,
                sLossLocationPostCode,
                sMarketReference,
                sOrigClaimReference,
                sPCSCode,
                sPlaintiffAttorneyReference,
                sPoliceIncidentReference,
                sPolicyRef,
                sTR,
                sUCR,
                sUMR,
                bPolicyPremiumPaid,
                bManuallyCorrectedComplete,
                dPolicySectionLinePC,
                dtDenied,
                sLossLocationLat,
                sLossLocationLong,
                sLossDateNotes,
                lPaperKey,
                sInsuredPhone,
                sInsuredEmail,
                sBrokerPhone,
                sBrokerEmail,
                sBrokerContactPhone,
                sBrokerContactEmail,
                sExternalAdjusterPhone,
                sExternalAdjusterEmail,
                nProofNotPresentedCount,
                lCombinedPolicyFolderKey,
                lCombinedPolicyKey,
                lCombinedPolicySectionKey,
                lCombinedPolicyActivityKey,
                lEntityKey,
                lInstanceKey,
                lAgreementSectionKey,
                dAgreementSectionLinePC,
                lCurrencyKeyAgrmt,
                dROEAgrmt,
                bCoinsuranceClaimAccounting,
                lPolicyLineKey,
                bCoinsuranceExists,
                bOverRideAttachmentRule,
                sVoyageFrom,
                sVoyageTo,
                sBackground,
                sCoverage,
                sDamages,
                sLiability,
                sPlanOfAction,
                sReserveCommentary,
                lPolicyLineChainKey,
                dMinSignedLine,
                dMaxSignedLine,
                bPolicyLeader,
                lPolicyTypeOfLeaderKey,
                nPolicyYearOfAccount,
                bBinderPolicy,
                lTypeOfClaimSeverityKey,
                lClaimHandlerKey,
                bCloseClaimAFP,
                bCannotPayClaimsLTR,
                dtReopenedDate,
                dtCreatedDate,
                dtClaimAmountAgreedDate,
                lClaimStatusKey,
                dtWithdrawnDate,
                bPolicyCoverageMandatory,
                bPolicySectionMandatory,
                lvPolicyMandatoryFlagsKey,
                lCombinedPolicyListKey,
                bDOLOverride,
                bAuthorisedCatCode,
                dtNextReviewedSixMonths,
                dtNextReviewed,
                bDOLEntered,
                lTypeOfRAGKey,
                dClaimPotential,
                lReinstatementCurrencyKey,
                dReinstatementROE,
                dReinstatementTotalReserve,
                dReinstatementTotalPaidToDate,
                dReinstatementTotalDue,
                dReinstatementTotalReserveReport,
                dReinstatementTotalPaidToDateReport,
                dReinstatementTotalDueReport,
                bManualDuplicateExists,
                lClaimGroupKey,
                sManualDuplicateUCRCheck,
                dtReopened,
                lTypeofEventType,
                dtBrokerClaimReceived,
                dtPolicySignedDate,
                bTreaty,
                bDeligatedAuthority,
                lSCAPKey,
                bAuthorisedEvent,
                bFullFollow,
                bNoHardCopyFile,
                sClaimant,
                lClaimPotentialCcyKey,
                bDivisionChange
            order by
                dw_loadts
        ) as dw_extract_order
    from
        claim
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lClaimKey,
        bBordereauClaim,
        bBureauClaim,
        bClaimOutsideTerm,
        bECFClaim,
        bfaconpolicy,
        bInlitigation,
        bLeader,
        bLOC,
        bLossDateTBA,
        bManualBlockIndicator,
        bManuallyCorrected,
        bNoticeToReinsurer,
        bOutOfScope,
        bPremiumFullyEarned,
        bRIApplies,
        bSalvage,
        bSigned,
        bSubrogation,
        bsurplustreatyonpolicy,
        BTOTALLOSS,
        bWordingDeficient,
        DCURRENTEXPENSES,
        dCurrentExpensesReserve,
        dCurrentFeePaid,
        dCurrentFeeReserve,
        DCURRENTINDEMNITY,
        dCurrentIndemnityReserve,
        dCurrentTotalPaid,
        dCurrentTotalReserve,
        dInitialExpensesReserve,
        dInitialFeeReserve,
        dInitialIndemnityReserve,
        dInitialTotalReserve,
        dLOCCurrentBalance,
        dLOCInitialBalance,
        dLOCTotalDrawings,
        dMonitorCurrentPaidTotal,
        dPolicyActivityROE,
        dRateOfExchange,
        dSectionLinePC,
        dSubrogationAmount,
        dSubrogationSettlementAmount,
        dtClaim1stReported,
        dtClaimAttached,
        dtClaimMadeFrom,
        dtClaimMadeTo,
        dtClosed,
        dtLossFrom,
        dtDiscovery,
        dtLastReviewed,
        dtLossTo,
        DTREPORTED,
        dtSubrogationDateCompleted,
        dtSubrogationDateStarted,
        lAddressLookupKey,
        lAgreementActivityKey,
        lBordereauLiabilityKey,
        lAgreementBordereauLocationKey,
        lAgreementBordereauPolicyKey,
        lBordereauTransClmCanAttachKey,
        lBordereauTransactionKey,
        lAgreementFolderKey,
        lAgreementInsuredKey,
        lAgreementYearKey,
        lBlobBackgroundKey,
        lBlobCoverageKey,
        lBlobDamagesKey,
        lBlobLiabilityKey,
        lBlobPlanOfActionKey,
        lBlobReserveCommentaryKey,
        lBrokerContactLiaisonKey,
        lBrokerKey,
        lBureauLeaderContactKey,
        lCatCodeKey,
        lClaimantKey,
        lClaimsCauseCategoryKey,
        LCLAIMSCAUSETYPEKEY,
        lClaimsLeadType,
        lCurrencyKey,
        lDivisionKey,
        lEventKey,
        lExternalAdjusterContactKey,
        lExternalAttorneyContactKey,
        lInsuredKey,
        lInternalAdjusterContactKey,
        lInternalAttorneyContactKey,
        lLossLocationCityKey,
        lLossLocationCountryKey,
        lLossLocationCountryRegionKey,
        lLossLocationPostCodeKey,
        lLossLocationStateKey,
        lPlaintiffAttorneyContactKey,
        lPolicyActivityCurrencyKey,
        lPolicyActivityKey,
        lPolicyBrokerKey,
        lPolicyBrokerLiaisonKey,
        lPolicyDivisionKey,
        lPolicyFolderKey,
        lPolicyInsuredKey,
        lPolicyKey,
        lPolicySectionKey,
        lPolicySubDivisionKey,
        lResponsiblePartyKey,
        lSlipLeaderContactKey,
        lSubDivisionKey,
        lSubEventKey,
        lTypeOfCauseKey,
        lTypeOfClaimClosedKey,
        lTypeOfLossKey,
        lTypeOfLossLocationKey,
        lTypeOfMarketSourceKey,
        lTypeOfNAICCodeKey,
        lTypeOfNoticeToReinsurerKey,
        lTypeOfPolicyActivityKey,
        lUnderwriterContactKey,
        lVesselKey,
        nAccidentYear,
        nInvalidClassCounter,
        nYearOfAccount,
        sBrokerReference1,
        sBrokerReference2,
        sClaimDescription,
        sClaimReference,
        sClassUCR,
        sExternalAdjusterReference,
        sExternalAttorneyReference,
        sFireIncidentReference,
        sLocationName,
        sLossCulprit,
        sLossLocationAddress1,
        sLossLocationAddress2,
        sLossLocationCity,
        sLossLocationPostCode,
        sMarketReference,
        sOrigClaimReference,
        sPCSCode,
        sPlaintiffAttorneyReference,
        sPoliceIncidentReference,
        sPolicyRef,
        sTR,
        sUCR,
        sUMR,
        bPolicyPremiumPaid,
        bManuallyCorrectedComplete,
        dPolicySectionLinePC,
        dtDenied,
        sLossLocationLat,
        sLossLocationLong,
        sLossDateNotes,
        lPaperKey,
        sInsuredPhone,
        sInsuredEmail,
        sBrokerPhone,
        sBrokerEmail,
        sBrokerContactPhone,
        sBrokerContactEmail,
        sExternalAdjusterPhone,
        sExternalAdjusterEmail,
        nProofNotPresentedCount,
        lCombinedPolicyFolderKey,
        lCombinedPolicyKey,
        lCombinedPolicySectionKey,
        lCombinedPolicyActivityKey,
        lEntityKey,
        lInstanceKey,
        lAgreementSectionKey,
        dAgreementSectionLinePC,
        lCurrencyKeyAgrmt,
        dROEAgrmt,
        bCoinsuranceClaimAccounting,
        lPolicyLineKey,
        bCoinsuranceExists,
        bOverRideAttachmentRule,
        sVoyageFrom,
        sVoyageTo,
        sBackground,
        sCoverage,
        sDamages,
        sLiability,
        sPlanOfAction,
        sReserveCommentary,
        lPolicyLineChainKey,
        dMinSignedLine,
        dMaxSignedLine,
        bPolicyLeader,
        lPolicyTypeOfLeaderKey,
        nPolicyYearOfAccount,
        bBinderPolicy,
        lTypeOfClaimSeverityKey,
        lClaimHandlerKey,
        bCloseClaimAFP,
        bCannotPayClaimsLTR,
        dtReopenedDate,
        dtCreatedDate,
        dtClaimAmountAgreedDate,
        lClaimStatusKey,
        dtWithdrawnDate,
        bPolicyCoverageMandatory,
        bPolicySectionMandatory,
        lvPolicyMandatoryFlagsKey,
        lCombinedPolicyListKey,
        bDOLOverride,
        bAuthorisedCatCode,
        dtNextReviewedSixMonths,
        dtNextReviewed,
        bDOLEntered,
        lTypeOfRAGKey,
        dClaimPotential,
        lReinstatementCurrencyKey,
        dReinstatementROE,
        dReinstatementTotalReserve,
        dReinstatementTotalPaidToDate,
        dReinstatementTotalDue,
        dReinstatementTotalReserveReport,
        dReinstatementTotalPaidToDateReport,
        dReinstatementTotalDueReport,
        bManualDuplicateExists,
        lClaimGroupKey,
        sManualDuplicateUCRCheck,
        dtReopened,
        lTypeofEventType,
        dtBrokerClaimReceived,
        dtPolicySignedDate,
        bTreaty,
        bDeligatedAuthority,
        lSCAPKey,
        bAuthorisedEvent,
        bFullFollow,
        bNoHardCopyFile,
        sClaimant,
        lClaimPotentialCcyKey,
        bDivisionChange,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lClaimKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
