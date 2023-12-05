with subclass_segementaion_from_policy as (
    
SELECT DISTINCT sub_class_segmentation 
FROM
"IGI_PROD_DW"."dbt_dev"."dmn_policy_part"

where ltrim(rtrim(isnull(sub_class_segmentation,''))) <> ''

)


select * from subclass_segementaion_from_policy