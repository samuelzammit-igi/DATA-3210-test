with

claim_section as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim_section" where _valid_to is null
),

final as (
    select
        lClaimSectionKey as claim_section_id,
        lClaimKey as inward_claim_id
    from
        claim_section
)

select * from final