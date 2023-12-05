

with

division as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_division" where _valid_to is null
),

relabelled as (
    select
        lDivisionKey as division_id,
        scode,
        case
            when division.scode = 'LON' then 1
            when division.scode = 'EUR' then 2
            else 0
        end as division
    from
        division
)

select * from relabelled