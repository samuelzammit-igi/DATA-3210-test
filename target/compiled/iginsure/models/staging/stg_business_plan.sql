with business_plan as (
    --seed
    select * from "IGI_PROD_DW"."dbt_dev"."business_plan"

),

business_plan_unpiv as (
    SELECT replace(lower(LoB),' ', '') as LoB,
        replace(lower(Subclass),' ', '') as Subclass,
        CONCAT(replace(lower(LoB),' ', '') ,'_', replace(lower(Subclass),' ', ''),'_',year) AS lob_subclass_year,
        year,
        SUM(GWP) AS GWP,
        SUM(GNWP) AS GNWP,
        SUM(GLR) AS GLR,
        SUM(NRM) AS NRM,
        SUM(Acq_Cost) AS Acq_Cost
        FROM (
        SELECT LoB, 
            Subclass, 
            type,
            right(type,4) as year,
            case when type like '%gwp%' then value else 0 end as GWP,
            case when type like '%gnwp%' then value else 0 end as GNWP,
            case when type like '%GLR%' then value else 0 end as GLR,
            case when type like '%nrm%' then value else 0 end as NRM,
            case when type like '%Acq%' then value else 0 end as Acq_Cost

        FROM   
        (SELECT     ltrim(rtrim(LoB)) as LoB,
                    ltrim(rtrim(Subclass)) as Subclass,
                    [BP GWP 2021],
                    [BP GWP 2022],
                    [BP GNWP 2021],
                    [BP GNWP 2022],
                    [BP GLR 2021],
                    [BP GLR 2022],
                    [BP GNLR 2021],
                    [BP GNLR 2022],
                    [BP Acq Costs 2021],
                    [BP Acq Costs 2022],
                    [BP NRM 2021],
                    [BP NRM 2022],
                    [BP Acq Costs % 2021],
                    [BP Acq Costs % 2022]
        FROM business_plan) p  
        UNPIVOT  
        (value FOR type IN ([BP GWP 2021],
                    [BP GWP 2022],
                    [BP GNWP 2021],
                    [BP GNWP 2022],
                    [BP GLR 2021],
                    [BP GLR 2022],
                    [BP GNLR 2021],
                    [BP GNLR 2022],
                    [BP Acq Costs 2021],
                    [BP Acq Costs 2022],
                    [BP NRM 2021],
                    [BP NRM 2022],
                    [BP Acq Costs % 2021],
                    [BP Acq Costs % 2022])  
    )AS unpvt)final

    GROUP BY LoB,Subclass,year
)

select * from business_plan_unpiv