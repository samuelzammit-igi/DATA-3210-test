

with pol (policy_id)
as (
    select policyref as policy_id
    from "IGI_PROD_DW"."dbt_dev"."actuarial_all_core_filtered"
),

premium as (
    select *
    from "IGI_PROD_DW"."dbo"."ActuarialCopy_Earned_Premium_Data_with_XOL"
),

filtered_by_policy as (
    select
        pol.policy_id,
        premium.gross_earned_premium as gross_earned_premium,
        premium.gross_acq_earned_premium as inward_acquisition_cost,
        --TODO confirm no agency fees included
        premium.qs_earned_premium as quota_share_premium,
        premium.qs_acq_earned_premium as quota_share_acquisition,
        premium.fac_earned_premium as fac_premium,
        premium.fac_acq_earned_premium as fac_acquisition,
        premium.allocated_xol_cost as allocated_xol_cost,
        -- we want to set the is_earned flag based on the control date
        -- i.e. only set is_earned to true if that premium would have 
        -- been considerd earned as-at the control date
        cast((
            case
                when convert(varchar, earning_quarter, 112) -- YYYYMMDD
                    <= '20231205' then 1
                else 0
            end
        )
        as bit) as is_earned,
        premium.earning_quarter as earning_qtr
    from pol
    inner join premium
        on pol.policy_id = premium.policy_number
),

aggregates as (
    select
        policy_id,
        earning_qtr,
        --TODO check that it's fine to use MAX
        --should not make a difference since value should be the same within grp
        max(cast(is_earned as int)) as is_earned,
        sum(gross_earned_premium) as gross_earned_premium,
        sum(inward_acquisition_cost) as inward_acquisition_cost,
        sum(quota_share_premium) as quota_share_premium,
        sum(quota_share_acquisition) as quota_share_acquisition,
        sum(fac_premium) as fac_premium,
        sum(fac_acquisition) as fac_acquisition,
        sum(allocated_xol_cost) as allocated_xol_cost
    from filtered_by_policy
    group by policy_id, earning_qtr
)

select * from aggregates