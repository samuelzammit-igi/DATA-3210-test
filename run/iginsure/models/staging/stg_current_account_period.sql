create view "dbt_szammit"."stg_current_account_period__dbt_tmp" as
    with account_period as (
    select * from "IGI_PROD_DW"."dbt_szammit"."scd_account_period" where _valid_to is null
),

current_acc as (  
    select min(nyearperiod) as AP
    from(
            select *, rank() over (partition by nyearperiod order by _valid_from desc) as rank  
            from account_period
        )a 
    where bclosing =0
    and rank=1
    )

select * from current_acc
