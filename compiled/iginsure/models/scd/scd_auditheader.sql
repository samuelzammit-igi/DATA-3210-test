

with auditheader as (

    select * from "IGI_PROD_DW"."dbo"."auditheader"

),


ordered as (
SELECT 
    LAUDITHEADERKEY,
    LSECURITYUSERKEY,
    LEDITMODE,
    NAUDITOBJECT,
    LAUDITOBJECTKEY,
    LAUDITOBJECTINSTANCEKEY,
    DTDATEAMENDED,
    SDELETEDOBJECTDESC,
    dw_loadts,
    row_number() over(partition by 
                    LAUDITHEADERKEY,
                    LSECURITYUSERKEY,
                    LEDITMODE,
                    NAUDITOBJECT,
                    LAUDITOBJECTKEY,
                    LAUDITOBJECTINSTANCEKEY,
                    DTDATEAMENDED,
                    SDELETEDOBJECTDESC
                    order by 
                    dw_loadts) as dw_extract_order

    FROM auditheader
    
    ),


filtered as (

    select * from ordered where dw_extract_order = 1

),


ranged as (

    SELECT
    LAUDITHEADERKEY,
    LSECURITYUSERKEY,
    LEDITMODE,
    NAUDITOBJECT,
    LAUDITOBJECTKEY,
    LAUDITOBJECTINSTANCEKEY,
    DTDATEAMENDED,
    SDELETEDOBJECTDESC,
    dw_loadts as _valid_from,
    lead(dw_loadts) over (partition by LAUDITHEADERKEY,LSECURITYUSERKEY,LEDITMODE,NAUDITOBJECT,LAUDITOBJECTKEY,LAUDITOBJECTINSTANCEKEY,DTDATEAMENDED,SDELETEDOBJECTDESC ORDER BY dw_loadts) AS _valid_to
    
    FROM 

    filtered

)


SELECT * FROM ranged