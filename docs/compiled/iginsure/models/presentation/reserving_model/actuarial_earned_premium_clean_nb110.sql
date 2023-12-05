-- transformations applied to the premium earning table in
-- notebook 110_Curate_FeatEng_Elements.ipynb



with premium as (
    select * from "IGI_PROD_DW"."dbt_dev"."actuarial_earned_premium_clean_nb100"
),

adding_csum as (
    select
        *,

        sum(gross_earned_premium)
        over (partition by policy_id order by earning_qtr)
        as gross_earned_premium_csum,

        sum(gross_earned_premium_acq)
        over (partition by policy_id order by earning_qtr)
        as gross_earned_premium_acq_csum,

        sum(qs_earned_premium)
        over (partition by policy_id order by earning_qtr)
        as qs_earned_premium_csum,

        sum(qs_earned_premium_acq)
        over (partition by policy_id order by earning_qtr)
        as qs_earned_premium_acq_csum,

        sum(fac_earned_premium)
        over (partition by policy_id order by earning_qtr)
        as fac_earned_premium_csum,

        sum(fac_earned_premium_acq)
        over (partition by policy_id order by earning_qtr)
        as fac_earned_premium_acq_csum,

        sum(xol_premium_allocated_acq)
        over (partition by policy_id order by earning_qtr)
        as xol_premium_allocated_acq_csum
    from premium
)

select
    *,
    -- GWP net of primary acquisition
    (gross_earned_premium - gross_earned_premium_acq) as gwp_ma,
    -- GWP net of primary and reinsurance acquisition costs
    (
        (gross_earned_premium - gross_earned_premium_acq)
        - (qs_earned_premium - qs_earned_premium_acq)
        - (fac_earned_premium - fac_earned_premium_acq)
        - xol_premium_allocated_acq
    ) as gwp_maa,
    -- cumulative GWP net of primary acquisition
    (gross_earned_premium_csum - gross_earned_premium_acq_csum) as gwp_ma_csum,
    -- cumulative GWP net of primary and reinsurance acquisition costs
    (
        (gross_earned_premium_csum - gross_earned_premium_acq_csum)
        - (qs_earned_premium_csum - qs_earned_premium_acq_csum)
        - (fac_earned_premium_csum - fac_earned_premium_acq_csum)
        - xol_premium_allocated_acq_csum
    ) as gwp_maa_csum
from adding_csum