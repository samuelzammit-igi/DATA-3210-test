

with

stg_contact as (
    select * from "IGI_PROD_DW"."dbt_dev"."stg_contact"
),

contact_link as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_contact_link" where _valid_to is null
),

broker_major_group as(
    select
        contact_link.LCONTACTKEY as broker_id,
        contact_link.LLINKTOCONTACTKEY as broker_major_group_id,
        stg_contact.contact_reference as broker_major_group_name
    from
        contact_link 
        inner join stg_contact on
            contact_link.LLINKTOCONTACTKEY = stg_contact.contact_id
    where
        contact_link.LLINKKEY = 2 -- Sub Broker of
)

select * from broker_major_group