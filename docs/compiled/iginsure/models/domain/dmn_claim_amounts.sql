

with
Inward_claims as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_inward_claims_amounts"
),

Outward_claims as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_outward_claims_amounts"
),

/*policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."dmn_policy"
),*/


IW_OW_Claims as (
    select * from Inward_claims
    union all
    select * from Outward_claims
)


select * from IW_OW_Claims

/*,

final as (
    select 
        iw_ow.sclaimreference as ClaimReference,
        p.sReference as PolicyRef,
        iw_ow.sCcy as OrgCcy,
        iw_ow.RIRef,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then TotalOSOrg else 0 end as InwardOSOrg,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then TotalPytsRcptsOrg else 0 end as InwardPaidOrg,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then TotalIncurredOrgCcy else 0 end as InwardIncurredOrg,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then TotalOSOrg else 0 end as FACOSOrg,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then TotalPytsRcptsOrg else 0 end as FACPaidOrg,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then TotalIncurredOrgCcy else 0 end as FACIncurredOrg,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then TotalOSOrg else 0 end as QSOSOrg,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then TotalPytsRcptsOrg else 0 end as QSPaidOrg,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS')then TotalIncurredOrgCcy else 0 end as QSIncurredOrg,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then TotalOsAcCcy else 0 end as InwardOSAcc,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then TotalPytsRcptsACCcy else 0 end as InwardPaidAcc,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then TotalIncurredACCcy else 0 end as InwardIncurredAcc,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then TotalOsAcCcy else 0 end as FACOSAcc,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then TotalPytsRcptsACCcy else 0 end as FACPaidAcc,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then TotalIncurredACCcy else 0 end as FACIncurredAcc,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then TotalOsAcCcy else 0 end as QSOSAcc,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then TotalPytsRcptsACCcy else 0 end as QSPaidAcc,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then TotalIncurredACCcy else 0 end as QSIncurredAcc,
        
        --QS Acc Exp/Ind
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then LastOSAmountACCcyExpense else 0 end as QSLastOSAcccyExpense,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then LastPytsRcptsAmountACCcyExpense else 0 end as QSLastPaidAcccyExpense,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then LastIncurredACCcyExpense else 0 end as QSLastIncAcccyExpense,        
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then LastOSAmountACCcyIND else 0 end as QSLastOSAcccyIND,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then LastPytsRcptsAmountACCcyIND else 0 end as QSLastPaidAcccyIND,
        case when (left(iw_ow.RIRef,1)='7' or left(iw_ow.RIRef,2)='OQ' or left(iw_ow.RIRef,2)='OS') then LastIncurredACCcyIND else 0 end as QSLastIncAcccyIND,

        --FAC Acc Exp/Ind
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then LastOSAmountACCcyExpense else 0 end as FACLastOSAcccyExpense,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then LastPytsRcptsAmountACCcyExpense else 0 end as FACLastPaidAcccyExpense,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then LastIncurredACCcyExpense else 0 end as FACLastIncAcccyExpense,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then LastOSAmountACCcyIND else 0 end as FACLastOSAcccyIND,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then LastPytsRcptsAmountACCcyIND else 0 end as FACLastPaidAcccyIND,
        case when (left(iw_ow.RIRef,1)='9' or left(iw_ow.RIRef,2)='OF' or left(iw_ow.RIRef,2)='FT') then LastIncurredACCcyIND else 0 end as FACLastIncAcccyIND,

        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then LastOSAmountACCcyExpense else 0 end as InwardOSACCExpense,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then LastOSAmountACCcyIND else 0 end as InwardOSACCIND,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then LastPytsRcptsAmountACCcyExpense else 0 end as InwardPaidACCExpense,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then LastPytsRcptsAmountACCcyIND else 0 end as InwardPaidACCIND,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then LastIncurredACCcyExpense else 0 end as InwardIncACCExpense,
        case when (iw_ow.RIRef='' or iw_ow.RIRef=NULL) then LastIncurredACCcyIND else 0 end as InwardIncACCIND

    from IW_OW_Claims iw_ow
        inner join policy p on iw_ow.lPolicyKey = p.lPolicyKey
)

select * from final*/