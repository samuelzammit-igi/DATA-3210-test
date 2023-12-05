
  
    
  
  if object_id ('"dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dim_Product_Subclass_Segmentation__dbt_tmp_temp_view] as
    with product_subclass_segmentation as (
    
SELECT * , CONCAT(line_product_name,'','',sub_class_segmentation) AS Product_Subclass_Segm_For_Join
    FROM 
    (SELECT DISTINCT line_product_name,sub_class_segmentation 
    FROM
    "IGI_PROD_DW"."dbt_szammit"."dmn_policy_part"

    where ltrim(rtrim(isnull(sub_class_segmentation,''''))) <> '''')final

)


select * from product_subclass_segmentation
    ');

  CREATE TABLE "dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dim_Product_Subclass_Segmentation__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_Product_Subclass_Segmentation__dbt_tmp_temp_view"
    end



  