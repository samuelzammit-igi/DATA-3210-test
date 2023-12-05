
  
    
  
  if object_id ('"dbt_szammit"."actuarial_all_core_filtered__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_all_core_filtered__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."actuarial_all_core_filtered__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."actuarial_all_core_filtered__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[actuarial_all_core_filtered__dbt_tmp_temp_view] as
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
    where [Include / Exclude] = ''Include''
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
            substring(''20230823'', 1, 6)
            as int)
        order by accountperiod desc
    )
)   

select * from filtered
    ');

  CREATE TABLE "dbt_szammit"."actuarial_all_core_filtered__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[actuarial_all_core_filtered__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."actuarial_all_core_filtered__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."actuarial_all_core_filtered__dbt_tmp_temp_view"
    end



  