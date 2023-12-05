with product_subclass_segmentation as (
    
SELECT * , CONCAT(line_product_name,',',sub_class_segmentation) AS Product_Subclass_Segm_For_Join
    FROM 
    (SELECT DISTINCT line_product_name,sub_class_segmentation 
    FROM
    "IGI_PROD_DW"."dbt_dev"."dmn_policy_part"

    where ltrim(rtrim(isnull(sub_class_segmentation,''))) <> '')final

)


select * from product_subclass_segmentation