-- for a given control date, we want to extract a subset of AllCore which would
-- represent the dataset as-at that date. We do this by first combining
-- AllCore and AllCore_Historical, finding the closest Account period to our
-- control date, and then filtering on that account period to get a snapshot
-- of the data at that point



with combined as (
    select * from "IGI_PROD_DW"."dbo"."ActuarialCopy_AllCore"    
    union all
    select * from "IGI_PROD_DW"."dbo"."ActuarialCopy_AllCore_Historical"
),

incepted as (
    select *
    from combined
    where [Include / Exclude] = 'Include'
),

filtered as (
    select *
    from incepted
    where accountperiod = (
        select top(1) accountperiod
        from combined
        -- the AllCore table uses YYYYMM as the date format,
        -- so we need to truncate from YYYYMMDD
        where accountperiod <= cast(
            substring('20231205', 1, 6)
            as int)
        order by accountperiod desc
    )
)   

select * from filtered