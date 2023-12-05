
  
    
  
  if object_id ('"dbt_szammit"."dim_iw_ow_flag__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_iw_ow_flag__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dim_iw_ow_flag__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dim_iw_ow_flag__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dim_iw_ow_flag__dbt_tmp_temp_view] as
    with iw_ow_flags as (


    select distinct iw_ow_flag from "IGI_PROD_DW"."dbt_szammit"."premium_data"
    where iw_ow_flag is not null


)

select * from iw_ow_flags
    ');

  CREATE TABLE "dbt_szammit"."dim_iw_ow_flag__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dim_iw_ow_flag__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dim_iw_ow_flag__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_iw_ow_flag__dbt_tmp_temp_view"
    end



  