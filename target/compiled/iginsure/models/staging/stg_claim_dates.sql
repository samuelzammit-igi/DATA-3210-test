with
current_account_period as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_current_account_period" 
),
claim_section as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim_section" where _valid_to is null
),
claim_movement as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim_movement" where _valid_to is null
),
currency as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_currency" where _valid_to is null
),

account_period as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_account_period" where _valid_to is null
),
entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_entity_instance_states" where entity_type_id = 445 --Claim
),


claim_main_dates as(
    select 
        C.lClaimKey,
        MAX(case when EISCM.entity_state_id in( 2262,2263) and CM.lTypeOfMovementKey in(10,21) /*Reserve Amendment*/ then CM.dtMovementCreated else '' end) as LastOSDate,
        MAX(case WHEN EISCM.entity_state_id in( 2262,2263) and CM.lTypeOfMovementKey not in(21,10) /*Initial Reserve,Reserve Amendment*/ then CM.dtMovementCreated else '' end) as LastPaidDate,
        MIN(case when EISCM.entity_state_id in( 2262,2263) and CM.lTypeOfMovementKey = 21 /*Initial Reserve*/ then CM.dtMovementCreated else '9999-12-31' end) as InitialOSDate
    from 
        claim C 
        left join claim_section CS on C.lClaimKey = CS.lClaimKey
        left join claim_movement CM on CS.lClaimSectionKey = CM.lClaimSectionKey
        left join account_period Acc_Period on CM.lAccountPeriodKey = Acc_Period.lAccountPeriodKey
        left join currency CU on CS.lReserveCcyOrigKey = CU.lCurrencyKey
        left join entity_instance_states EISCM on CM.lClaimMovementKey = EISCM.instance_id
	where Acc_Period.nYearPeriod<=(select AP from current_account_period)
    group by 
        C.lClaimKey
)
select * from claim_main_dates