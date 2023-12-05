create view "dbt_dev"."scd_auditrows__dbt_tmp" as
    

with auditrows as (

    select * from "IGI_PROD_DW"."dbo"."auditrows"

),


ordered as (
SELECT 
    LAUDITROWSKEY,
    LAUDITHEADERKEY,
    LCHANGETYPE,
    LAUDITOBJECTINSTANCEKEY,
    LAUDITOBJECTKEY,
    SDELETEDOBJECTDESC,
    dw_loadts,
    row_number() over(partition by 
                        LAUDITROWSKEY,
                        LAUDITHEADERKEY,
                        LCHANGETYPE,
                        LAUDITOBJECTINSTANCEKEY,
                        LAUDITOBJECTKEY,
                        SDELETEDOBJECTDESC,
                        dw_loadts
                    order by 
                    dw_loadts) as dw_extract_order

    FROM auditrows
    
    ),


filtered as (

    select * from ordered where dw_extract_order = 1

),


ranged as (

    SELECT
    LAUDITROWSKEY,
    LAUDITHEADERKEY,
    LCHANGETYPE,
    LAUDITOBJECTINSTANCEKEY,
    LAUDITOBJECTKEY,
    SDELETEDOBJECTDESC,
    dw_loadts as _valid_from,
    lead(dw_loadts) over (partition by LAUDITROWSKEY,LAUDITHEADERKEY,LCHANGETYPE,LAUDITOBJECTINSTANCEKEY,LAUDITOBJECTKEY,SDELETEDOBJECTDESC,dw_loadts ORDER BY dw_loadts) AS _valid_to
   
    FROM 

    filtered

)


SELECT * FROM ranged
