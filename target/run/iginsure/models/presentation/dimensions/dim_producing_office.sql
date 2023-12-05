
  
    
  
  if object_id ('"dbt_dev"."dim_producing_office__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_dev"."dim_producing_office__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_dev"."dim_producing_office__dbt_tmp"','U') is not null
    begin
    drop table "dbt_dev"."dim_producing_office__dbt_tmp"
    end


   EXEC('create view [dbt_dev].[dim_producing_office__dbt_tmp_temp_view] as
    with producing_office_from_source as (


    select distinct line_producing_office as producing_office from 
    "IGI_PROD_DW"."dbt_dev"."dmn_policy_part"

    where line_producing_office is not null and line_producing_office <> ''Not Available''

)


select * from producing_office_from_source
    ');

  CREATE TABLE "dbt_dev"."dim_producing_office__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_dev].[dim_producing_office__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_dev"."dim_producing_office__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_dev"."dim_producing_office__dbt_tmp_temp_view"
    end



  