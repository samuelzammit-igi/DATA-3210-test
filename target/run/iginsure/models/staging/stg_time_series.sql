create view "dbt_szammit"."stg_time_series__dbt_tmp" as
    with digits as (
    select cast(value as integer) as number from string_split('0,1,2,3,4,5,6,7,8,9', ',')
),

numbers as (
    select
        thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number as number
    from
        digits as ones
        cross join digits as tens
        cross join digits as hundreds
        cross join digits as thousands
),

dates as (
    select
        dateadd(day, number, '2000-01-01') as date
    from
        numbers
),

spine as (
    select
        cast(date as date) as date,
        dateadd(year, datediff(year, 0, date), 0) as year,
        dateadd(quarter, datediff(quarter, 0, date), 0) as quarter,
        dateadd(month, datediff(month, 0, date), 0) as month,
        dateadd(week, datediff(week, 0, date), 0) as week,
        datepart(dayofyear, date) as day_of_year,
        datepart(day, date) as day_of_month,
        datepart(weekday, date) as weekday
    from
        dates
),

periods as (
    select
        *,
        dateadd(year, 1, year) as yoy1,
        dateadd(year, 2, year) as yoy2,
        dateadd(year, 3, year) as yoy3,
        dateadd(year, 1, quarter) as yoy1_by_quarter,
        dateadd(year, 2, quarter) as yoy2_by_quarter,
        dateadd(year, 3, quarter) as yoy3_by_quarter,
        dateadd(year, 1, month) as yoy1_by_month,
        dateadd(year, 2, month) as yoy2_by_month,
        dateadd(year, 3, month) as yoy3_by_month,
        dateadd(month, 1, month) as mom1,
        dateadd(month, 2, month) as mom2,
        dateadd(month, 3, month) as mom3
    from
        spine
)

select * from periods
