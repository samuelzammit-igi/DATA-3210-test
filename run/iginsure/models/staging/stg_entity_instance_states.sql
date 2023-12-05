create view "dbt_dev"."stg_entity_instance_states__dbt_tmp" as
    

with

entity_type as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_entity_type" where _valid_to is null
),

entity_state_members as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_entity_state_members" where _valid_to is null
),

entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_entity_instance_states" where _valid_to is null
),

entity_states as (
    select
        entity_type."LENTITYKEY" as entity_type_id,
        entity_type."SENTITY" as entity_type,
        entity_state_members."LENTITYSTATEMEMBERKEY" as entity_state_id,
        entity_state_members."SENTITYSTATEMEMBER" entity_state

    from
        entity_type
        left join entity_state_members on
            entity_type."LENTITYSTATEKEY" = entity_state_members."LENTITYSTATEKEY"
),

instances_states_values as (
    select
        entity_instance_states."lInstanceKey" as instance_id,
        entity_states.entity_type_id,
        entity_states.entity_type,
        entity_states.entity_state_id,
        entity_states.entity_state
    from
        entity_instance_states
        inner join entity_states on
            entity_instance_states."lEntityStateMemberKey" = entity_states.entity_state_id
            and entity_instance_states."lEntityKey" = entity_states.entity_type_id
)

select * from instances_states_values
