create view "dbt_szammit"."scd_ri_section_broker__dbt_tmp" as
    -- This file is automatically generated

with

ri_section_broker as (
    select * from "IGI_PROD_DW"."dbo"."RISectionBroker"
),

ordered as (
    select
        lRISectionBrokerKey,
        lRISectionKey,
        lRIActivityKey,
        lRIPolicyKey,
        lRIFolderKey,
        lBrokerContactKey,
        sReference,
        sBrokerReference,
        lBrokerLiaisonKey,
        dBrokerOrderPc,
        dBrokerOrderPlaced,
        dBrokerCommissionPc,
        dCedingCommissionPc,
        dOverridingCommissionPc,
        nTermsOfTradeDays,
        sBrokerName,
        dBrokerRemunerationPc,
        bSecurityLevelStatement,
        lRISectionCoBrokerKey,
        lBrokerClaimLiaisonKey,
        intBrokerOrderXOLInfo,
        dw_loadts,
        row_number() over (
            partition by
                lRISectionBrokerKey,
                lRISectionKey,
                lRIActivityKey,
                lRIPolicyKey,
                lRIFolderKey,
                lBrokerContactKey,
                sReference,
                sBrokerReference,
                lBrokerLiaisonKey,
                dBrokerOrderPc,
                dBrokerOrderPlaced,
                dBrokerCommissionPc,
                dCedingCommissionPc,
                dOverridingCommissionPc,
                nTermsOfTradeDays,
                sBrokerName,
                dBrokerRemunerationPc,
                bSecurityLevelStatement,
                lRISectionCoBrokerKey,
                lBrokerClaimLiaisonKey,
                intBrokerOrderXOLInfo
            order by
                dw_loadts
        ) as dw_extract_order
    from
        ri_section_broker
),

filtered as (
    select * from ordered where dw_extract_order = 1
),

ranged as (
    select
        lRISectionBrokerKey,
        lRISectionKey,
        lRIActivityKey,
        lRIPolicyKey,
        lRIFolderKey,
        lBrokerContactKey,
        sReference,
        sBrokerReference,
        lBrokerLiaisonKey,
        dBrokerOrderPc,
        dBrokerOrderPlaced,
        dBrokerCommissionPc,
        dCedingCommissionPc,
        dOverridingCommissionPc,
        nTermsOfTradeDays,
        sBrokerName,
        dBrokerRemunerationPc,
        bSecurityLevelStatement,
        lRISectionCoBrokerKey,
        lBrokerClaimLiaisonKey,
        intBrokerOrderXOLInfo,
        dw_loadts as _valid_from,
        lead(dw_loadts) over (partition by lRISectionBrokerKey order by dw_loadts) as _valid_to
    from
        filtered
)

select * from ranged
