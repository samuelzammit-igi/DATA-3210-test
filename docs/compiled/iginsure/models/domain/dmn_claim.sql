with 

claim as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim" where _valid_to is null
),

claim_section as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim_section" where _valid_to is null
),

claim_movement as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim_movement" where _valid_to is null
),

type_of_event as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_event" where _valid_to is null
),

type_of_event_type as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_event_type" where _valid_to is null
),

type_of_rag as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_rag" where _valid_to is null
),

zone_country as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_zone_country" where _valid_to is null
),

stg_contact as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_contact"
),

type_of_loss as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_loss" where _valid_to is null
),

stg_entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_entity_instance_states" where entity_type = 'Claim Movement' 
),

currency as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_currency" where _valid_to is null
),

account_period as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_account_period" where _valid_to is null
),

claim_dates as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_claim_dates"
),

linked_claim as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_claimgroup_linkedclaims"
),

dmn_inward_claim as(
    select
        claim.lClaimKey as claim_id,
        claim.lPolicykey as policy_id,
        claim.lPolicyLineKey as policy_line_id,
        claim.lPolicyActivityKey as policy_activity_id,
        claim_section.lClaimSectionKey as claim_section_id,
        claim_movement.lClaimMovementKey as claim_movement_id,
        claim.sClaimReference as claim_reference,
        claim.sUCR as ucr,
        claim.sClaimDescription as claim_description,
        claim.sPoliceIncidentReference as police_incident_ref,
        claim.dtLossFROM as date_of_loss_from,
        claim.DTREPORTED as date_notified,
        type_of_event.SEVENTCODE as event_code,
        type_of_event.SEVENTDESC as event_description,
        type_of_event_type.sTypeofEventDescr as event_type,
        torag.sTypeofRAGDescr as rag,
        claim.dClaimPotential as claim_potential,
        loss_country.sCountry as loss_location_country,
        claim.dtClosed as closed_date,
        case when claim.bInlitigation = 0 then 'NO' else 'YES' end as inlitigation,
        adjuster_contact.contact_reference as adjustor,
        type_of_loss.STYPEOFLOSS as type_of_loss,
        claim.dtLastReviewed as last_reviewed,
        claim.nAccidentYear as accident_year,
        claim_state.entity_state as claim_status,
        claim_handler_contact.contact_reference as claim_handler,
        claim.bBureauClaim as bureau_claim,
        claim.dtNextReviewed as review_date,
        currency.sCcy as section_orig_currency,
        account_period.lAccountPeriodKey as account_period_id,
        account_period.nYearPeriod as account_period,
        case
            when claim_movement_state.entity_state in ('Authorised', 'Paid')
            then claim_movement.dTotalResChangeAuthOrigShare
            else 0
        end as os_orig,
        case
            when claim_movement_state.entity_state in ('Authorised', 'Paid')
            then 
                case
                    when claim_movement.ltypeofmovementkey in(18,2000001,2000025,2000021,16)
                    then -1 * (claim_movement.dPaidThisTimeOrigShare)
                    else (claim_movement.dPaidThisTimeOrigShare)
                end 
            else 0
        end as paid_orig,
        claim_movement.dPayROEOrig as paid_roe,
        claim_movement_state.entity_state as claim_movement_status,
        case
            when claim_movement.lTransactionTypeKey = 15 /*Indemnity*/
            then 'indemnity'
            else 'expense'
        end as section_type,
       claim.nYearOfAccount,
       claim_dates.LastOSDate,
       claim_dates.LastPaidDate,
       claim_dates.InitialOSDate,
       case when lc.parent_claim_reference is null then claim.sclaimreference else lc.parent_claim_reference end parent_claim_reference
    from
        claim
        inner join claim_section on 
            claim.lClaimKey = claim_section.lClaimKey
        inner join claim_dates on 
            claim.lClaimKey = claim_dates.lClaimKey
        inner join claim_movement on 
            claim_section.lClaimSectionKey = claim_movement.lClaimSectionKey 
        inner join account_period on 
            claim_movement.lAccountPeriodKey = account_period.lAccountPeriodKey
        left join type_of_event on 
            claim.lEventKey = type_of_event.LEVENTKEY
        left join type_of_event_type on 
            claim.lTypeofEventType = type_of_event_type.lTypeofEventKey
        left join type_of_rag torag on 
            claim.lTypeOfRAGKey = torag.lTypeOfRAGKey
        left join zone_country loss_country on 
            claim.lLossLocationCountryKey = loss_country.lCountryKey
        left join stg_contact as adjuster_contact on 
            claim.lExternalAdjusterContactKey = adjuster_contact.contact_id
        left join type_of_loss on
            claim.lTypeOfLossKey = type_of_loss.LTYPEOFLOSSKEY
        left join stg_entity_instance_states as claim_state on 
            claim.lClaimKey = claim_state.instance_id
            and claim_state.entity_type = 'Claim'
        left join stg_contact as claim_handler_contact on 
            claim.lClaimHandlerKey = claim_handler_contact.contact_id
        left join currency on 
            claim_section.lReserveCcyOrigKey = currency.lCurrencyKey
        left join stg_entity_instance_states as claim_movement_state on 
            claim_movement.lClaimMovementKey = claim_movement_state.instance_id
        left join linked_claim lc on claim.lclaimkey = lc.claim_id
            
)

select * from dmn_inward_claim