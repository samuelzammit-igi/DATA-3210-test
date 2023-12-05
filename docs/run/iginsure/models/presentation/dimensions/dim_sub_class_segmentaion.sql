
  
    
  
  if object_id ('"dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp_temp_view"
    end


   
    
  if object_id ('"dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp"','U') is not null
    begin
    drop table "dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp"
    end


   EXEC('create view [dbt_szammit].[dim_sub_class_segmentaion__dbt_tmp_temp_view] as
    with subclass_segementaion_from_policy as (
    
SELECT DISTINCT sub_class_segmentation 
FROM
"IGI_PROD_DW"."dbt_szammit"."dmn_policy_part"

where ltrim(rtrim(isnull(sub_class_segmentation,''''))) <> ''''

)


select * from subclass_segementaion_from_policy
    ');

  CREATE TABLE "dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp"
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      CLUSTERED COLUMNSTORE INDEX
      )
    AS (SELECT * FROM [dbt_szammit].[dim_sub_class_segmentaion__dbt_tmp_temp_view])

   
  
  if object_id ('"dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp_temp_view"','V') is not null
    begin
    drop view "dbt_szammit"."dim_sub_class_segmentaion__dbt_tmp_temp_view"
    end



  