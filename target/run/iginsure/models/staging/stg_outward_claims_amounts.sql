create view "dbt_szammit"."stg_outward_claims_amounts__dbt_tmp" as
    

with
claim as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_claim" where _valid_to is null
),

claim_section as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_claim_section" where _valid_to is null
),

claim_movement as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_claim_movement" where _valid_to is null
),

account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_account_period" where _valid_to is null
),

currency as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_currency" where _valid_to is null
),

entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_entity_instance_states" where entity_type_id = 531 --Claim Movement
),

ri_policy as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_ri_policy" where _valid_to is null
),

ri_policy_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_ri_policy_period" where _valid_to is null
),

ri_policy_period_prop as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_ri_policy_period_prop" where _valid_to is null and lEntityKey = 531 --Claim Movement
),

current_account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_current_account_period" 
),

current_exchange_rate as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_current_exchange_rate" 

),

clm_audit_state_transitions as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_claim_mvmt_audit_state_transitions"
),


initial_ow as 
(
    select
        C.lClaimKey,
        C.sClaimReference,
        C.lPolicyKey,
	    CU.sCcy,
        COALESCE(ast.DTTRANSITION,CM.dtMovementCreated) as auth_paid_date ,
	    RIP.sReference as RIRef,
        tri.scode as TypeOfRI,
		case when EISCM.entity_state_id in( 2262,2263) then RIPPP.dInwardsOSMovement else 0 end as TotalOSOrg ,
		case when EISCM.entity_state_id in( 2262,2263) then RIPPP.dInwardsRecovery else 0 end as TotalPytsRcptsOrg, 

		case when EISCM.entity_state_id in( 2262,2263) then cast(RIPPP.dInwardsOSMovement / EXRATE_OS.dROE as decimal(18,2) ) else 0 end as TotalOsAcCcy ,
		case when EISCM.entity_state_id in( 2262,2263) then cast(RIPPP.dInwardsRecovery / RIPPP.dInwardsROE as decimal(18,2) ) else 0 end as TotalPytsRcptsACCcy ,

        ----OS IND /EXP Org ccy
        case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 15 /*Indemnity*/ then  RIPPP.dInwardsOSMovement else 0 end as LastOSAmountOrgIND, --New calculation
		case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 16 /*Expense*/ then  RIPPP.dInwardsOSMovement else 0 end as LastOSAmountOrgCcyExpense,

		----Paid IND /EXP Org ccy
        case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 15 /*Indemnity*/ then RIPPP.dInwardsRecovery else 0 end as LastPytsRcptsAmountOrgIND, --New Calculation
        case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 16 /*Expense*/ then RIPPP.dInwardsRecovery else 0 end as LastPytsRcptsAmountOrgExpense, --New Calculation

		----Paid IND /EXP Acc ccy
		case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 15 /*Indemnity*/ then cast(RIPPP.dInwardsRecovery / RIPPP.dInwardsROE as decimal(18,2) ) else 0 end as LastPytsRcptsAmountACCcyIND,
		case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 16 /*Expense*/ then cast(RIPPP.dInwardsRecovery / RIPPP.dInwardsROE as decimal(18,2) ) else 0 end as LastPytsRcptsAmountACCcyExpense,

		----OS IND /EXP Acc ccy
		case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 15 /*Indemnity*/ then cast(RIPPP.dInwardsOSMovement / EXRATE_OS.dROE as decimal(18,2) ) else 0 end as LastOSAmountACCcyIND,
		case when EISCM.entity_state_id in( 2262,2263) and CM.lTransactionTypeKey = 16 /*Expense*/ then cast(RIPPP.dInwardsOSMovement / EXRATE_OS.dROE as decimal(18,2) ) else 0 end as LastOSAmountACCcyExpense  

    from
       	claim C 
        inner join claim_section CS on C.lClaimKey = CS.lClaimKey
        inner join claim_movement CM on CS.lClaimSectionKey = CM.lClaimSectionKey
        --inner join AccountPeriod Acc_Period on CM.lAccountPeriodKey = Acc_Period.lAccountPeriodKey
        inner join currency CU on CS.lReserveCcyOrigKey = CU.lCurrencyKey
        inner join entity_instance_states EISCM on CM.lClaimMovementKey = EISCM.instance_id 
        inner join current_exchange_rate EXRATE_OS on EXRATE_OS.lCurrencyKey = CM.lReserveCcyOrigKey
        inner join ri_policy_period_prop RIPPP on CM.lClaimMovementKey = RIPPP.lClaimMovementKey
        inner join ri_policy_period RI_Period on RIPPP.lRIPolicyPeriodKey = RI_Period.lRIPolicyPeriodKey
        inner join account_period Acc_Period on RI_Period.lAccountPeriodKey = Acc_Period.lAccountPeriodKey
        inner join ri_policy RIP on RIPPP.lRIPolicyKey = RIP.lRIPolicyKey
        left join  clm_audit_state_transitions ast on CM.lClaimMovementKey = ast.linstancekey
        inner JOIN RIActivity RIA ON RIPPP.lRIActivityKey = RIA.lRIActivityKey
		LEFT JOIN TypeOfRiPolicy tri ON tri.LTYPEOFRIPOLICYKEY = RIA.lTypeOfRIPolicyKey
        INNER JOIN RISection RIS ON RIA.lRIActivityKey = RIS.lRIActivityKey

    where 
        --C.lClaimKey NOT IN(SELECT DISTINCT ISNULL(lClaimKey,0) as ClaimKey FROM #BureauClaims UNIon SELECT DISTINCT ISNULL(lLinkedClaimKey,0) as ClaimKey from #BureauClaims_LinkedClaims)
		--AND 
		--RIPPP.lEntityKey = case when C.bBureauClaim = -1 AND CM.lTypeOfMovementKey NOT IN(10,21) /*Reserve Amendment,Initial Reserve*/ then 1359 else 531 end
		Acc_Period.nYearPeriod<=(select AP from current_account_period)
),

outward_amount as (
    select  OW_Amt.lClaimKey as claim_id,
            OW_Amt.sClaimReference as claim_reference,
            OW_Amt.lPolicyKey as policy_id,
            OW_Amt.sCcy,
            OW_Amt.auth_paid_date,
            OW_Amt.RIRef,
            OW_Amt.TypeOfRI,
            sum(isnull(OW_Amt.TotalOSOrg, 0))                       as TotalOSOrg,
            sum(isnull(OW_Amt.TotalOsAcCcy, 0))                     as TotalOsAcCcy,
            sum(isnull(OW_Amt.LastOSAmountOrgCcyExpense, 0))        as LastOSAmountOrgCcyExpense,
            sum(isnull(OW_Amt.LastOSAmountACCcyExpense, 0))         as LastOSAmountACCcyExpense,
            sum(isnull(OW_Amt.LastOSAmountOrgIND, 0))               as LastOSAmountOrgIND, -- new calculation
            sum(isnull(OW_Amt.LastOSAmountACCcyIND, 0))             as LastOSAmountACCcyIND,
            sum(isnull(OW_Amt.TotalPytsRcptsOrg, 0))                as TotalPytsRcptsOrg,
            sum(isnull(OW_Amt.TotalPytsRcptsACCcy, 0))              as TotalPytsRcptsACCcy,
            sum(isnull(OW_Amt.LastPytsRcptsAmountOrgExpense, 0))    as LastPytsRcptsAmountOrgExpense, -- new calculation
            sum(isnull(OW_Amt.LastPytsRcptsAmountACCcyExpense, 0))  as LastPytsRcptsAmountACCcyExpense,
            sum(isnull(OW_Amt.LastPytsRcptsAmountOrgIND, 0))        as LastPytsRcptsAmountOrgIND, -- new calculation
            sum(isnull(OW_Amt.LastPytsRcptsAmountACCcyIND, 0))      as LastPytsRcptsAmountACCcyIND,
        
            -- Incurred calculations
            sum(isnull(OW_Amt.TotalOSOrg, 0)) + sum(isnull(OW_Amt.TotalPytsRcptsOrg, 0))                                as TotalIncurredOrgCcy,
            sum(isnull(OW_Amt.TotalOsAcCcy, 0)) + sum(isnull(OW_Amt.TotalPytsRcptsACCcy, 0))                            as TotalIncurredACCcy,
            sum(isnull(OW_Amt.LastOSAmountOrgCcyExpense, 0)) + sum(isnull(OW_Amt.LastPytsRcptsAmountOrgExpense, 0))     as LastIncurredOrgExpense,
            sum(isnull(OW_Amt.LastOSAmountACCcyExpense, 0)) + sum(isnull(OW_Amt.LastPytsRcptsAmountACCcyExpense, 0))    as LastIncurredACCcyExpense,
            sum(isnull(OW_Amt.LastOSAmountOrgIND, 0)) + sum(isnull(OW_Amt.LastPytsRcptsAmountOrgIND, 0))                as LastIncurredOrgIND,
            sum(isnull(OW_Amt.LastOSAmountACCcyIND, 0)) + sum(isnull(OW_Amt.LastPytsRcptsAmountACCcyIND, 0))            as LastIncurredACCcyIND
            
    from    initial_ow OW_Amt
    group by 
            OW_Amt.lClaimKey,
            OW_Amt.sClaimReference,
            OW_Amt.lPolicyKey,
            OW_Amt.sCcy,
            OW_Amt.auth_paid_date,
            OW_Amt.RIRef,
            OW_Amt.TypeOfRI
            
)


select * from outward_amount
