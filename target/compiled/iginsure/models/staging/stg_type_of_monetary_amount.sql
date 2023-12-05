

with

toma as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_type_of_monetary_amount" where _valid_to is null
),

stg_toma as (
    select
        toma.lTypeOfMonetaryAmountKey as type_of_monetary_amount_id,
        toma.sCode as toma_code,
        toma.sTypeOfMonetaryAmount as type_of_monetary_amount,
        case
            when toma.sCode in (
                'BKG','COM','FEE','ODD',
                'ORC','TAX','SRB','AMF',
                'INT','PDF','CDM','AGD',
                'AGU','AGN','AGE','MUM',
                'FIN','SEG','RBT','PCM',
                'TRL','SI_SUR','SI_SURI','AGO'
            )
            then cast('TRUE' as bit)
            else cast('FALSE' as bit)
        end as is_acq_cost,
        case
            when toma.sCode in (
                '759','502','517','APR_RIP',
                '503','505','753','ORI','WKA',
                'WKB','WKO','RBC','501'
            )
            then cast('TRUE' as bit)
            else cast('FALSE' as bit)
        end as is_gross_igi_share,
        case
            when toma.sCode in (
                'IPT','SRB','FIN','MUM','AGE',
                'SEG','PRL','PRT','GST','GSB','TAXIPT'
            )
            then cast('TRUE' as bit)
            else cast('FALSE' as bit)
        end as is_internal_deductions,
        case
            when toma.sCode = 'AGE'
            then cast('TRUE' as bit)
            else cast('FALSE' as bit)
        end as is_eio,
        case
            when toma.sCode in (
                'BKG','COM','FEE','ODD','ORC',
                'TAX','SRB','AMF','INT','PDF',
                'CDM','AGD','AGU','AGN','AGE',
                'MUM','FIN','SEG','759','502',
                '517','APR_RIP','503','RBT','PCM',
                '505','753','WKA','WKB','WKO','RBC',
                'ORI','TRL','501','SI_SUR','SI_SURI','AGO'
            )
            then cast('TRUE' as bit)
            else cast('FALSE' as bit)
        end as is_net_premium_exclude_ipt
    from 
        toma
)

select * from stg_toma