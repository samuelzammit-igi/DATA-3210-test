-- transformations applied to the policy table in notebook
-- 110_Curate_FeatEng_Elements.ipynb




with calculating_policy_duration_days as (
    select
        *,
        cast(uw_year as int) as uw_y,
        concat('s', cast(uw_year as varchar)) as uw_ys,
        datediff(dayofyear, inception_date, expiry_date) as policy_duration_days,
        (2022 - year(mis_uw_year)) as years_since_mis_uw_year
    from "IGI_PROD_DW"."dbt_dev"."actuarial_policy_clean_nb100"
)

select
    *,
    case
        when policy_duration_days in (364, 365, 366) then '1yr'
        when policy_duration_days < 364 then '<1yr'
        when policy_duration_days > 366 then '>1yr'
    end as policy_duration_grp
from calculating_policy_duration_days