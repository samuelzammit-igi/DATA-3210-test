with 

ri_policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_ri_policy" where _valid_to is null
),

ri_activity as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_ri_activity" where _valid_to is null
),

ri_section as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_ri_section" where _valid_to is null
),

ri_section_broker as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_ri_section_broker" where _valid_to is null
),

ri_section_broker_security as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_ri_section_broker_security" where _valid_to is null
),

type_of_ri_policy as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_ri_policy" where _valid_to is null
),

stg_contact as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_contact"
),

stg_broker_group as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_broker_group"
),

type_of_fronting as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_fronting" where _valid_to is null
),

stg_entity_instance_states as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_entity_instance_states"
),

type_of_ri_class as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_ri_class" where _valid_to is null
),


ri_policy_part as(
    select 
        ri_policy.lRIPolicyKey as ri_policy_id,
        ri_policy.sReference as ri_policy,
        ri_policy.dtPeriodFrom as ri_policy_inception,
        ri_policy.dtPeriodTo as ri_policy_expiry,
        type_of_ri_policy.SCODE as type_of_ri_policy_code,
        type_of_ri_policy.SPOLICYTYPE as type_of_ri_policy,
        ri_activity.lRIActivityKey as ri_activity_id,
        ri_section.lRISectionKey as ri_section_id,
        ri_section.dLimit as ri_pml_usd,
        ri_section.dLimit100 as inward_event_limit_share_usd,
        cast(case when ri_section.dROE = 0 then 0 else ri_section.dEventLimit/ri_section.dROE end as decimal(18,2)) as ri_event_limit_usd,
        cast(case when ri_section.dROE = 0 then 0 else ri_section.dExcess/ri_section.dROE end as decimal(18,2)) AS ri_excess_usd,
        cast(ri_section.dLimit * (ri_section_broker_security.dProportionPc/100) as decimal(18,2)) as participant_exposure,
        type_of_fronting.sFrontingCode as reason_code,
        type_of_fronting.sFrontingDescr as reason_description,
        ri_section_broker.lRISectionBrokerKey as ri_section_broker_id,
        ri_section_broker_security.lRISectionBrokerSecurityKey as ri_section_broker_security_id,
        ow_broker_contact.contact_id as ow_broker_id,
        ow_broker_contact.contact_reference as ow_broker,
        ow_broker_group.broker_major_group_name as ow_broker_major_group,
        year(ri_policy.dtPeriodFrom) as mis_treaty,
        ow_security_contact.contact_reference as ow_security,
        ri_section_broker.dBrokerOrderPc as broker_cession_pc,
        ri_section_broker_security.dProportionPc as security_cession_pc,
        ri_policy_entity_instance_states.entity_state as ri_policy_state,
        ri_activity_entity_instance_states.entity_state as ri_activity_state,
        type_of_ri_class.sTypeofRIClassDescr as type_of_ri_class_description
    from 
        ri_policy
        inner join ri_activity on 
            ri_policy.lRIPolicyKey = ri_activity.lRIPolicyKey
        inner join ri_section on 
            ri_activity.lRIActivityKey = ri_section.lRIActivityKey
        inner join ri_section_broker on 
            ri_section.lRISectionKey = ri_section_broker.lRISectionKey
        inner join ri_section_broker_security on 
            ri_section_broker.lRISectionBrokerKey = ri_section_broker_security.lRISectionBrokerKey
        inner join type_of_ri_policy on
            ri_activity.lTypeofRIPolicyKey = type_of_ri_policy.lTypeofRIPolicyKey
        left join stg_contact as ow_broker_contact on
            ri_section_broker.lBrokerContactKey = ow_broker_contact.contact_id
        left join stg_broker_group ow_broker_group on 
            ri_section_broker.lBrokerContactKey = ow_broker_group.broker_id
        left join stg_contact as ow_security_contact on 
            ri_section_broker_security.lSecurityContactKey = ow_security_contact.contact_id
        left join type_of_fronting on 
            ri_section.lFrontingKey = type_of_fronting.lFrontingKey
        left join stg_entity_instance_states as ri_policy_entity_instance_states on
            ri_policy.lRIPolicyKey = ri_policy_entity_instance_states.instance_id
            and ri_policy_entity_instance_states.entity_type = 'RI Policy'
        left join stg_entity_instance_states as ri_activity_entity_instance_states on 
            ri_activity.lRIActivityKey = ri_activity_entity_instance_states.instance_id
            and ri_activity_entity_instance_states.entity_type = 'RI Activity'
        left join type_of_ri_class on 
            ri_section.lTypeofRIClassKey = type_of_ri_class.lTypeofRIClassKey
)

select * from ri_policy_part