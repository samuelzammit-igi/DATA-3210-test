



with



claim_ as (

    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim" where _valid_to is null

),



claim_movement as (

    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim_movement" where _valid_to is null

),

policy_activity as (

    select * from "IGI_PROD_DW"."dbt_dev"."scd_policy_activity" where _valid_to is null

),



c_m_a as(

    SELECT  cm.lClaimMovementKey as claim_movement_id,

            cm.lpolicyactivitykey as policy_activity_id

    FROM claim_movement cm

    INNER join policy_activity pa ON cm.lpolicyactivitykey = pa.lpolicyactivitykey

   

),

c_m_c_a as (

    SELECT  cm.lClaimMovementKey as claim_movement_id,

            c.lPolicyActivityKey as policy_activity_id

    FROM claim_movement cm

    inner join claim c ON cm.lclaimkey = c.lclaimkey

    WHERE isnull(cm.lPolicyActivityKey,0) NOT IN (SELECT DISTINCT isnull(lpolicyactivitykey,0) FROM policy_activity)

),



stg_c_m as (

    select * from c_m_a

    union all

    select * from c_m_c_a

)



select * from stg_c_m