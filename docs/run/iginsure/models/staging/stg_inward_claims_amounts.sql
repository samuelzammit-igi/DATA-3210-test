create view "dbt_szammit"."stg_inward_claims_amounts__dbt_tmp" as
    

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

current_account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_current_account_period" 
),


current_exchange_rate as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_current_exchange_rate" 

),

auth_mvmt_pending_financial as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_auth_mvmt_pending_financial" 

),

entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_entity_instance_states" 
    where entity_type_id = 531 --Claim Movement
),

clm_audit_state_transitions as (
    select * from "IGI_PROD_DW"."dbt_szammit"."stg_claim_mvmt_audit_state_transitions"
),




inward_amount as 
(
--select col1,col2, (col1+col2) as col3 from test11
select IN_Amt2.*,
IN_Amt2.TotalOSOrg+IN_Amt2.TotalPytsRcptsOrg AS TotalIncurredOrgCcy,
IN_Amt2.TotalOsAcCcy+IN_Amt2.TotalPytsRcptsAcCcy AS TotalIncurredACCcy,
IN_Amt2.LastOSAmountOrgCcyExpense+IN_Amt2.LastPytsRcptsAmountOrgExpense AS LastIncurredOrgExpense,
IN_Amt2.LastOSAmountAcCcyExpense+IN_Amt2.LastPytsRcptsAmountACCcyExpense AS LastIncurredACCcyExpense,
IN_Amt2.LastOSAmountOrgIND+IN_Amt2.LastPytsRcptsAmountOrgIND AS LastIncurredOrgIND,
IN_Amt2.LastOSAmountACCcyIND+IN_Amt2.LastPytsRcptsAmountACCcyIND AS LastIncurredACCcyIND


 from (

    select 
    IN_Amt.lClaimKey as claim_id,
    IN_Amt.sclaimreference as claim_reference,
    IN_Amt.lPolicyKey as policy_id,
	IN_Amt.sCcy,
    IN_Amt.auth_paid_date,
    IN_Amt.RIRef,
    In_Amt.TypeOfRI,

	SUM(ISNULL(IN_Amt.TotalOSOrg,0)) AS TotalOSOrg,
	SUM(ISNULL(IN_Amt.TotalOsAcCcy,0)) AS TotalOsAcCcy,


	SUM(ISNULL(IN_Amt.LastOSAmountOrgCcyExpense,0)) AS LastOSAmountOrgCcyExpense,
	SUM(ISNULL(IN_Amt.LastOSAmountAcCcyExpense,0)) AS LastOSAmountAcCcyExpense,

   
	SUM(ISNULL(IN_Amt.LastOSAmountOrgIND,0)) AS LastOSAmountOrgIND,
    SUM(ISNULL(IN_Amt.LastOSAmountACCcyIND,0)) AS LastOSAmountACCcyIND,

    SUM(ISNULL(IN_Amt.TotalPytsRcptsOrg,0)) AS TotalPytsRcptsOrg,
    SUM(ISNULL(IN_Amt.TotalPytsRcptsAcCcy,0)) AS TotalPytsRcptsAcCcy,

    SUM(ISNULL(IN_Amt.LastPytsRcptsAmountOrgExpense,0)) AS LastPytsRcptsAmountOrgExpense,
    SUM(ISNULL(IN_Amt.LastPytsRcptsAmountACCcyExpense,0)) AS LastPytsRcptsAmountACCcyExpense,

    SUM(ISNULL(IN_Amt.LastPytsRcptsAmountOrgIND,0)) AS LastPytsRcptsAmountOrgIND,
    SUM(ISNULL(IN_Amt.LastPytsRcptsAmountACCcyIND,0)) AS LastPytsRcptsAmountACCcyIND

    from (
    select
        C.lClaimKey,
        C.sclaimreference,
        C.lPolicyKey,
        CU.sCcy,
        COALESCE(ast.DTTRANSITION,CM.dtMovementCreated) as auth_paid_date ,
        '' AS RIRef,
        'GROSS' AS TypeOfRI,
        --General OS (Org,Acc)
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) THEN (CM.dTotalResChangeAuthOrigShare) ELSE 0 END AS TotalOSOrg,
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) THEN CAST(CM.dTotalResChangeAuthOrigShare / EXRATE_OS.dROE AS DECIMAL (18,2)) ELSE 0 END AS TotalOsAcCcy ,
        --Expense OS (Org,Acc)
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 16 /*Expense*/ THEN (CM.dTotalResChangeAuthOrigShare) ELSE 0 END AS LastOSAmountOrgCcyExpense,
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 16 /*Expense*/ THEN CAST(CM.dTotalResChangeAuthOrigShare / EXRATE_OS.dROE AS DECIMAL (18,2)) ELSE 0 END AS LastOSAmountAcCcyExpense,
        --Indemnity OS (Org,Acc)
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 15 /*Indemnity*/ THEN (CM.dTotalResChangeAuthOrigShare) ELSE 0 END AS LastOSAmountOrgIND,
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 15 /*Indemnity*/ THEN CAST(CM.dTotalResChangeAuthOrigShare / EXRATE_OS.dROE AS DECIMAL (18,2) ) ELSE 0 END AS LastOSAmountACCcyIND,
		

        --General Paid (Org,Acc)
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) THEN(CASE WHEN CM.ltypeofmovementkey in(18,2000001,2000025,2000021,16)THEN -1* (CM.dPaidThisTimeOrigShare)ELSE (CM.dPaidThisTimeOrigShare)END   )ELSE 0 END AS TotalPytsRcptsOrg,
		CASE WHEN ESMCM.entity_state_id  in( 2262,2263) THEN(CASE WHEN CM.ltypeofmovementkey in(18,2000001,2000025,2000021,16)THEN -1* CAST(CM.dPaidThisTimeOrigShare / CM.dPayROEOrig AS DECIMAL (18,2) )ELSE CAST(CM.dPaidThisTimeOrigShare / CM.dPayROEOrig AS DECIMAL(18,2) )END ) ELSE 0 END AS TotalPytsRcptsAcCcy,
        --Expense Paid(Org,Acc)
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 16 /*Expense*/ THEN (CASE WHEN CM.ltypeofmovementkey in(18,2000001,2000025,2000021,16)THEN -1* (CM.dPaidThisTimeOrigShare)ELSE (CM.dPaidThisTimeOrigShare) END   )ELSE 0 END AS LastPytsRcptsAmountOrgExpense,
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 16 /*Expense*/ THEN(CASE WHEN CM.ltypeofmovementkey in(18,2000001,2000025,2000021,16)THEN -1* CAST(CM.dPaidThisTimeOrigShare / CM.dPayROEOrig AS DECIMAL (18,2) )ELSE CAST(CM.dPaidThisTimeOrigShare / CM.dPayROEOrig AS DECIMAL(18,2) )END ) ELSE 0 END AS LastPytsRcptsAmountACCcyExpense,
        --Indemnity Paid (Org,Acc)
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 15 /*Indemnity*/ THEN(CASE WHEN CM.ltypeofmovementkey in(18,2000001,2000025,2000021,16)THEN -1* (CM.dPaidThisTimeOrigShare)ELSE(CM.dPaidThisTimeOrigShare )END) ELSE 0 END AS LastPytsRcptsAmountOrgIND,
        CASE WHEN ESMCM.entity_state_id  in( 2262,2263) AND CM.lTransactionTypeKey = 15 /*Indemnity*/ THEN(CASE WHEN CM.ltypeofmovementkey in(18,2000001,2000025,2000021,16)THEN -1* CAST(CM.dPaidThisTimeOrigShare / CM.dPayROEOrig AS DECIMAL (18,2) )ELSE CAST(CM.dPaidThisTimeOrigShare / CM.dPayROEOrig AS DECIMAL(18,2) )END ) ELSE 0 END AS LastPytsRcptsAmountACCcyIND




    from
        claim as c
        left join claim_section as CS on
            C.lClaimKey = CS.lClaimKey 
        LEFT JOIN claim_movement as CM ON
            CS.lClaimSectionKey = CM.lClaimSectionKey
        LEFT JOIN account_period as Acc_Period ON 
            CM.lAccountPeriodKey = Acc_Period.lAccountPeriodKey
        LEFT JOIN currency as CU ON 
            CS.lReserveCcyOrigKey = CU.lCurrencyKey
        LEFT join entity_instance_states ESMCM 
          on CM.lClaimMovementKey = ESMCM.instance_id
        LEFT JOIN current_exchange_rate EXRATE_OS 
          on EXRATE_OS.lCurrencyKey=CM.lReserveCcyOrigKey
        LEFT JOIN clm_audit_state_transitions ast 
          on CM.lClaimMovementKey = ast.linstancekey


    where (Acc_Period.nYearPeriod<=(select AP from current_account_period)) 
    and (CM.lClaimMovementKey NOT IN(SELECT ISNULL(lClaimMovementKey,0) AS lClaimMovementKey FROM auth_mvmt_pending_financial))

    
    )IN_Amt
    GROUP BY 
    IN_Amt.lClaimKey,
	IN_Amt.sclaimreference,
    IN_Amt.lPolicyKey,
	IN_Amt.sCcy,
    IN_Amt.auth_paid_date,
    IN_Amt.RIRef,
    IN_Amt.TypeOfRI

)IN_Amt2
    
    
)

select * from inward_amount
