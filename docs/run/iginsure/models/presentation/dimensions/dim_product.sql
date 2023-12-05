
  
    
  
  if object_id ('"dbt_szammit"."dim_product__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_product__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dim_product__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dim_product__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dim_product__dbt_tmp_temp_view] as
    with product_from_source as (


select distinct product_name
from "IGI_PROD_DW"."dbt_szammit"."stg_product"

where product_name is not null

)

select * from product_from_source
    ');

  CREATE TABLE "dbt_szammit"."dim_product__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dim_product__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dim_product__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_product__dbt_tmp_temp_view"
    end



  