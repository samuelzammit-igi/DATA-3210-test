

with

contact as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_contact" where _valid_to is null
),

relabelled as (
    select
        "lContactKey" as contact_id,
        "sContactReference" as contact_reference,
        "sCompanyName" as company_name,
        "sEmail" as email,
        "sToBeKnownAs" as to_be_known_as
    from
        contact
)

select * from relabelled