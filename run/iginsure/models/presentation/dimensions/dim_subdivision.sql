
  
    
  
  if object_id ('"dbt_szammit"."dim_subdivision__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_subdivision__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dim_subdivision__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dim_subdivision__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dim_subdivision__dbt_tmp_temp_view] as
    with subdivision_from_source AS (

    select distinct subdivision,subdivision_desc from 
    "IGI_PROD_DW"."dbt_szammit"."stg_subdivision"
    where subdivision is not null

)

select * from subdivision_from_source
    ');

  CREATE TABLE "dbt_szammit"."dim_subdivision__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dim_subdivision__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dim_subdivision__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_subdivision__dbt_tmp_temp_view"
    end



  