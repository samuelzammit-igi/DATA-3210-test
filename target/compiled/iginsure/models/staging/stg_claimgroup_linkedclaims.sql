

with

claimgrouplinkedclaims as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claimgrouplinkedclaims" where _valid_to is null
),
claim as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_claim" where _valid_to is null
), 

claimlinkedclaim as (
    select clc.lclaimkey as parent_claim_id,
    p_c.sclaimreference as parent_claim_reference,
    clc.lLinkedClaimKey as claim_id,
    c_c.sclaimreference as claim_reference

    from claimgrouplinkedclaims clc 
    inner join claim p_c on clc.lclaimkey = p_c.lclaimkey
    inner join claim c_c on clc.lLinkedClaimKey = c_c.lclaimkey
)

select * from claimlinkedclaim