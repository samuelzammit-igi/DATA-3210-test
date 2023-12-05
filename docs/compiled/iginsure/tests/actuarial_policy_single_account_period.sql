-- All rows in actuarial_policy_clean_nb110 should belong to a single 
-- acount period. This test simply grabs the first account period it finds
-- and returns any rows which do not belong to this period. This way,
-- the test fails if there are multiple distinct values for the account period 

select * from "IGI_PROD_DW"."dbt_dev"."actuarial_policy_clean_nb110" where account_period != (
    select top(1) account_period 
    from "IGI_PROD_DW"."dbt_dev"."actuarial_policy_clean_nb110"
)