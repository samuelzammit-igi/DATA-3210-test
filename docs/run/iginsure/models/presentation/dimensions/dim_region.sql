
  
    
  
  if object_id ('"dbt_szammit"."dim_region__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_region__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dim_region__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dim_region__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dim_region__dbt_tmp_temp_view] as
    with region_from_policy as (

SELECT distinct territory as region
FROM
"IGI_PROD_DW"."dbt_szammit"."stg_country_territory"
where territory is not null

)

select * from region_from_policy
    ');

  CREATE TABLE "dbt_szammit"."dim_region__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dim_region__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dim_region__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_region__dbt_tmp_temp_view"
    end



  