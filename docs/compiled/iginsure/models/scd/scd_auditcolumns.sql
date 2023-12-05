

with auditcolumns as (

    select * from "IGI_PROD_DW"."dbo"."auditcolumns"

),


ordered as (
SELECT 
    LAUDITCOLUMNSKEY,
    LAUDITHEADERKEY,
    LAUDITROWSKEY,
    LENTITYPROPERTYKEY,
    SFIELDNAME,
    DOLDVALUE,
    DTOLDVALUE,
    SOLDVALUE,
    sOldValueDescription,
    dw_loadts,
    row_number() over(partition by 
                    LAUDITCOLUMNSKEY,
                    LAUDITHEADERKEY,
                    LAUDITROWSKEY,
                    LENTITYPROPERTYKEY,
                    SFIELDNAME,
                    DOLDVALUE,
                    DTOLDVALUE,
                    SOLDVALUE,
                    sOldValueDescription,
                    dw_loadts
                    order by 
                    dw_loadts) as dw_extract_order

    FROM auditcolumns
    
    ),


filtered as (

    select * from ordered where dw_extract_order = 1

),


ranged as (

SELECT
    LAUDITCOLUMNSKEY,
    LAUDITHEADERKEY,
    LAUDITROWSKEY,
    LENTITYPROPERTYKEY,
    SFIELDNAME,
    DOLDVALUE,
    DTOLDVALUE,
    SOLDVALUE,
    sOldValueDescription,
    dw_loadts as _valid_from,
    lead(dw_loadts) over (partition by LAUDITCOLUMNSKEY,LAUDITHEADERKEY,LAUDITROWSKEY,LENTITYPROPERTYKEY,SFIELDNAME,DOLDVALUE,DTOLDVALUE,SOLDVALUE,sOldValueDescription ORDER BY dw_loadts) AS _valid_to
   
    FROM 

    filtered

)


SELECT * FROM ranged