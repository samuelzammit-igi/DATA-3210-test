create view "dbt_dev"."stg_subdivision__dbt_tmp" as
    


with

subdivision as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_sub_division" where _valid_to is null
),

relabelled as (
    select
        "lSubDivisionKey" as subdivision_id,
        case sCode
            when 'JD' then 'JOR'
            when 'BE' then 'BER'
            when 'LA' then 'LAB'
            when 'DU' then 'DUB'
            when 'CS' then 'CAS'
            when 'TK' then 'TAK'
            when 'LO' then 'LON'
            when 'EU' then 'EUR'
            when 'NR' then 'NOR'
            else 'NA'
        end as subdivision,

        sSubDivision as subdivision_desc
    from
        subdivision
)

select * from relabelled
