


with iris_mvmt as (

    select * from "IGI_PROD_DW"."iris"."IRIS_INW_GWP_Movemnets"

),


segmentation as (
    select * from "IGI_PROD_DW"."dbo"."Segmentation"
),


iris_mvmts_reformated as (SELECT 
	0 as policy_id,
	CONCAT('iris','_',ROW_NUMBER() OVER (ORDER BY (SELECT 1))) as apr_reference,
	1 as apr_activity_id,
	CASE WHEN type_of_monetary_amount in('premium_usd','acq_cost_usd','eio_fees_usd','argo_fees_usd') then apr_amount else 0 end as apr_amount_usd,

    CASE WHEN type_of_monetary_amount = 'premium_usd' then apr_amount else 0 end as iw_gross,
    CASE WHEN type_of_monetary_amount = 'acq_cost_usd' then apr_amount else 0 end as iw_acq,

    --original amounts
    CASE WHEN type_of_monetary_amount in('premium_org','acq_cost_org','eio_fees_org','argo_fees_org') then apr_amount else 0 end as apr_amount_org,
    Ccy as apr_org_ccy,
    ROE as apr_roe,
    CASE WHEN type_of_monetary_amount = 'premium_org' then apr_amount else 0 end as iw_gross_org,
    CASE WHEN type_of_monetary_amount = 'acq_cost_org' then apr_amount else 0 end as iw_acq_org,


	-- end of month account period
	eomonth(DATEFROMPARTS(LEFT(AccountPeriod,4),RIGHT(AccountPeriod,2),1)) as apr_entry_date,
	--NULL as apr_allocation_date,
    NULL as apr_settlement_due_date,
    AccountPeriod as apr_account_period,
	RIGHT(AccountPeriod,2) as AP_month,
	LEFT(AccountPeriod,4) as AP_year,
	CAST(DATEFROMPARTS(LEFT(AccountPeriod,4),RIGHT(AccountPeriod,2),1) as DATE) as AP_Date,
	--'Paid' as apr_status,
	CASE
        WHEN type_of_monetary_amount in('premium_org','premium_usd')  THEN 'Gross premium ( + )'
        WHEN type_of_monetary_amount in ('acq_cost_org','acq_cost_usd') THEN 'Brokerage ( - )'
		WHEN type_of_monetary_amount in('eio_fees_org','eio_fees_usd')  THEN 'Agency Fees EIO'
		WHEN type_of_monetary_amount in('argo_fees_org','argo_fees_usd')  THEN 'Intermediary Fees'
    END as type_of_monetary_amount,
    '' As activity_version,
    '' as activity_type,
	1 as effective_activity_id,
	CASE
        WHEN Division = 'London' THEN 1 
		WHEN Division = 'Bermuda' THEN 0
    END as activity_division,
    CASE
        WHEN Subdivision = 'JD' THEN 'BER' -- subdivision: combine Jordan and Bermuda
        WHEN Subdivision = 'CS' THEN 'CAS'
        WHEN Subdivision = 'TK' THEN 'TAK'
        WHEN Subdivision = 'LO' THEN 'LON'
        WHEN Subdivision = 'BE' THEN 'BER'
        WHEN Subdivision = 'DU' THEN 'DUB'
        WHEN Subdivision = 'LA' THEN 'LAB'
    END as Subdivision,

    XFI_Product as product,

    case
        when XFI_Product in (
            'AVIATION','FINANCIAL INSTITUTIONS','INWARDS PPN','INWARDS XOL','MARINE LIABILITY',
            'MARINE TRADE','POLITICAL VIOLENCE','PORTS & TERMINALS','PROPERTY','CONTINGENCY'
        )
        then XFI_Product

        when XFI_Product = 'CASUALTY'
        then
            case
                when 'PROFESSIONAL INDEMNITY' = coverage
                and substring(PolicyReference,1,6) <> '600494'
                then 'PROFESSIONAL INDEMNITY' 

                when 'PROFESSIONAL INDEMNITY' = coverage
                and substring(PolicyReference,1,6) = '600494'
                then 'PEN' 

                when REPLACE(coverage,'&amp;','&') in('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY' , 'PUBLIC OFFERING OF SECURITIES INSURANCE')
                then 'DIRECTORS & OFFICERS'

                when coverage in ('MEDICAL MALPRACTICE','SURETY BONDS')
                then coverage

                when 'LEGAL EXPENSES' = coverage
                AND   Subclass like '%BTE%' 
                then 'LEGAL EXPENSES BTE'

                when 'LEGAL EXPENSES' = coverage
                AND   Subclass like '%ATE%' 
                then 'LEGAL EXPENSES ATE'

                else 'CGL'
            end

        when XFI_Product = 'DOWNSTREAM ENERGY'
        then
            case
                when class in ('POWER GENERATION-CONVENTIONAL','POWER GENERATION-RENEWABLES')
                then 'POWER & RENEWABLES'
                else 'OIL & GAS'
            end

        when XFI_Product = 'ENGINEERING'
        then
            case
                when coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
                then 'IDI'
                else 'ENGINEERING'
            end

        when XFI_Product = 'FORESTRY'
        then 'PROPERTY'

        when XFI_Product = 'MARINE'
        then 'MARINE CARGO'

        when XFI_Product = 'UPSTREAM ENERGY'
        then
            case
                when class in ('CAR','EIO CAR')
                then 'UPSTREAM CONSTRUCTION'
                else 'UPSTREAM ENERGY'
            end

        when XFI_Product = 'MR'
        then 'INWARDS XOL'
    end as activity_SubClassSegmentation,

	XFI_Product as product_segregation,
	Coverage as activity_coverage_description,
	coverage_code as activity_coverage_code,
	class as activity_section_class,
	AccountPeriod as activity_written_account_period,
	PolicyReference as policy_reference,
    '' as activity_source,
    NULL as broker_version_number,
    
    'INWARD' as iw_ow_flag,
    '' as ri_policy,
     Null as ri_policy_inception,
     null as ri_policy_expiry,
     '' as ow_broker,
     '' as ow_broker_major_group,
     '' as ow_security,



	1 as for_date_activity_id,
	CASE
        WHEN Division = 'London' THEN 1 
		WHEN Division = 'Bermuda' THEN 0
    END as for_date_division,
	
    CASE
        WHEN Subdivision = 'JD' THEN 'JOR'
	    WHEN Subdivision = 'CS' THEN 'CAS'
		WHEN Subdivision = 'TK' THEN 'TAK'
		WHEN Subdivision = 'LO' THEN 'LON'
		WHEN Subdivision = 'BE' THEN 'BER'
		WHEN Subdivision = 'DU' THEN 'DUB'
		WHEN Subdivision = 'LA' THEN 'LAB'
    END as subdivision_day,
	
	ProducingOffice as ProducingOffice_day,
	XFI_Product as for_date_product_name,
	XFI_Product as ProductSegregation_day,
 	
	case
        when XFI_Product in (
            'AVIATION','DOWNSTREAM ENERGY','FINANCIAL INSTITUTIONS','FORESTRY','INWARDS PPN',
            'INWARDS XOL','MARINE LIABILITY','MARINE TRADE','MR','POLITICAL VIOLENCE',
            'PORTS & TERMINALS','PROPERTY','UPSTREAM ENERGY','CONTINGENCY'
        )
        then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product)

        when XFI_Product = 'ENGINEERING' and Coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
        then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product and seg.Coverage = unpvt.Coverage)
        
        when XFI_Product = 'ENGINEERING' and Coverage not in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
        then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product and seg.Coverage not in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI'))

        when XFI_Product = 'CASUALTY'
        then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product and seg.Coverage = unpvt.Coverage)

        -- this product depends on classfication and we dont have it mapped. And in anycase from XFI side we dont have marinme segmentated
        when XFI_Product = 'MARINE'
        then XFI_Product
    
    end as segmentation_day,

    case
        when XFI_Product in (
            'AVIATION','FINANCIAL INSTITUTIONS','INWARDS PPN','INWARDS XOL','MARINE LIABILITY','MARINE TRADE',
            'POLITICAL VIOLENCE','PORTS & TERMINALS','PROPERTY','CONTINGENCY'
        )
        then XFI_Product

        when XFI_Product = 'CASUALTY'
        then
            case
                when 'PROFESSIONAL INDEMNITY' = coverage and 
                substring(PolicyReference,1,6) <> '600494'
                then 'PROFESSIONAL INDEMNITY'

                when 'PROFESSIONAL INDEMNITY'  = coverage and 
                substring(PolicyReference,1,6) = '600494'
                then 'PEN'

                when REPLACE(coverage,'&amp;','&') in ('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY','PUBLIC OFFERING OF SECURITIES INSURANCE')
                then 'DIRECTORS & OFFICERS'
                when REPLACE(coverage,'&amp;','&') in ('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY','PUBLIC OFFERING OF SECURITIES INSURANCE')
                then 'DIRECTORS & OFFICERS'

                when coverage in('MEDICAL MALPRACTICE')
                then coverage
                
                when 'LEGAL EXPENSES' = coverage
                AND   Subclass like '%BTE%' 
                then 'LEGAL EXPENSES BTE'

                when 'LEGAL EXPENSES'  = coverage
                AND   Subclass like '%ATE%' 
                then 'LEGAL EXPENSES ATE'

                when 'SURETY BONDS'  = coverage
                then 'FINANCIAL INSTITUTIONS'
                
                else 'CGL'
            end

        when XFI_Product = 'DOWNSTREAM ENERGY'
        then
            case
                when class in ('POWER GENERATION-CONVENTIONAL','POWER GENERATION-RENEWABLES')
                then 'POWER & RENEWABLES'
                else 'OIL & GAS'
            end

        when XFI_Product = 'ENGINEERING'
        then
            case
                when coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
                then 'IDI'
                else 'ENGINEERING'
            end

        when XFI_Product = 'FORESTRY'
        then 'PROPERTY'

        when XFI_Product = 'MARINE'
        then 'MARINE CARGO'

        when XFI_Product = 'UPSTREAM ENERGY'
        then
            case
                when class in ('CAR','EIO CAR')
                then 'UPSTREAM CONSTRUCTION'
                else 'UPSTREAM ENERGY'
            end

        when XFI_Product = 'MR'
        then 'INWARDS XOL'
    end as BudgetSegmentation_day,
	
    case
        when XFI_Product in (
            'AVIATION','FINANCIAL INSTITUTIONS','INWARDS PPN','INWARDS XOL','MARINE LIABILITY',
            'MARINE TRADE','POLITICAL VIOLENCE','PORTS & TERMINALS','PROPERTY','CONTINGENCY'
        )
        then XFI_Product

        when XFI_Product = 'CASUALTY'
        then
            case
                when 'PROFESSIONAL INDEMNITY' = coverage
                and substring(PolicyReference,1,6) <> '600494'
                then 'PROFESSIONAL INDEMNITY' 

                when 'PROFESSIONAL INDEMNITY' = coverage
                and substring(PolicyReference,1,6) = '600494'
                then 'PEN' 

                when REPLACE(coverage,'&amp;','&') in('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY' , 'PUBLIC OFFERING OF SECURITIES INSURANCE')
                then 'DIRECTORS & OFFICERS'

                when coverage in ('MEDICAL MALPRACTICE','SURETY BONDS')
                then coverage

                when 'LEGAL EXPENSES' = coverage
                AND   Subclass like '%BTE%' 
                then 'LEGAL EXPENSES BTE'

                when 'LEGAL EXPENSES' = coverage
                AND   Subclass like '%ATE%' 
                then 'LEGAL EXPENSES ATE'

                else 'CGL'
            end

        when XFI_Product = 'DOWNSTREAM ENERGY'
        then
            case
                when class in ('POWER GENERATION-CONVENTIONAL','POWER GENERATION-RENEWABLES')
                then 'POWER & RENEWABLES'
                else 'OIL & GAS'
            end

        when XFI_Product = 'ENGINEERING'
        then
            case
                when coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
                then 'IDI'
                else 'ENGINEERING'
            end

        when XFI_Product = 'FORESTRY'
        then 'PROPERTY'

        when XFI_Product = 'MARINE'
        then 'MARINE CARGO'

        when XFI_Product = 'UPSTREAM ENERGY'
        then
            case
                when class in ('CAR','EIO CAR')
                then 'UPSTREAM CONSTRUCTION'
                else 'UPSTREAM ENERGY'
            end

        when XFI_Product = 'MR'
        then 'INWARDS XOL'
    end as SubClassSegmentation_day,

	class as Class_day,
	producer_source as ProducerSource_day,
	producer as producer_day,
	ProducerGroup as ProducerGroup_day,
	'' as InsuranceType_day,
	Coverage as coverageDesc_day,
	coverage_code as CoverageCode_day,
	Territory as territory_day,
	DomicileCountry as domicile_day,
	Region as region_day,
	MISUWY as MISUWY_day,
	UWY as UWY_day,
	ASSURED as insured_day,
	REASSURED as reassured_day,
	FILE_HANDLER as FileHandler_day,
	UNDERWRITER as underwriter_day,
	REC_UWR as RecUnderwriter_day,
	ADMIN_FILE_HANDLER as AdminFileHandler_day,
	'' as OperatorId_day,
	'' as activity_notes_day,

    PolicyInceptionDate as for_date_policy_inception,

    PolicyExpiryDate as for_date_policy_expiry,
    PolicyExpiryDate as for_date_policy_expiry_reported,
    '' as for_date_New_vs_Renwal,

	1 as for_period_activity_id,

	CASE
        WHEN Division = 'London' THEN 1
        WHEN Division = 'Bermuda' THEN 0
    END as for_period_division,
	
	CASE
        WHEN Subdivision = 'JD' THEN 'BER' -- subdivision: combine Jordan and Bermuda
	    WHEN Subdivision = 'CS' THEN 'CAS'
		WHEN Subdivision = 'TK' THEN 'TAK'
		WHEN Subdivision = 'LO' THEN 'LON'
		WHEN Subdivision = 'BE' THEN 'BER'
		WHEN Subdivision = 'DU' THEN 'DUB'
		WHEN Subdivision = 'LA' THEN 'LAB'
    END as for_period_subdivision,

	ProducingOffice as ProducingOffice_month,
	
	XFI_Product as for_period_product_name,

	XFI_Product as ProductSegregation_month,

        case
            when XFI_Product in (
                'AVIATION','DOWNSTREAM ENERGY','FINANCIAL INSTITUTIONS','FORESTRY','INWARDS PPN',
                'INWARDS XOL','MARINE LIABILITY','MARINE TRADE','MR','POLITICAL VIOLENCE',
                'PORTS & TERMINALS','PROPERTY','UPSTREAM ENERGY','CONTINGENCY'
            )
            then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product)

            when XFI_Product = 'ENGINEERING' and Coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
            then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product and seg.Coverage = unpvt.Coverage)
            
			when XFI_Product = 'ENGINEERING' and Coverage not in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
            then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product and seg.Coverage not in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI'))

            when XFI_Product = 'CASUALTY'
            then (select distinct [Grouping] from segmentation seg where seg.Product = XFI_Product and seg.Coverage = unpvt.Coverage)

			-- this product depends on classfication and we dont have it mapped. And in anycase from XFI side we dont have marinme segmentated
            when XFI_Product = 'MARINE'
            then XFI_Product
        
		end as segmentation_month,

	  case
            when XFI_Product in (
                'AVIATION','FINANCIAL INSTITUTIONS','INWARDS PPN','INWARDS XOL','MARINE LIABILITY','MARINE TRADE',
                'POLITICAL VIOLENCE','PORTS & TERMINALS','PROPERTY','CONTINGENCY'
            )
            then XFI_Product

            when XFI_Product = 'CASUALTY'
            then
                case
                    when 'PROFESSIONAL INDEMNITY' = coverage and 
                    substring(PolicyReference,1,6) <> '600494'
                    then 'PROFESSIONAL INDEMNITY'

                    when 'PROFESSIONAL INDEMNITY'  = coverage and 
                    substring(PolicyReference,1,6) = '600494'
                    then 'PEN'

                    when REPLACE(coverage,'&amp;','&') in ('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY','PUBLIC OFFERING OF SECURITIES INSURANCE')
                    then 'DIRECTORS & OFFICERS'
                    when REPLACE(coverage,'&amp;','&') in ('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY','PUBLIC OFFERING OF SECURITIES INSURANCE')
                    then 'DIRECTORS & OFFICERS'

                    when coverage in('MEDICAL MALPRACTICE')
                    then coverage
                    
                    when 'LEGAL EXPENSES' = coverage
                    AND   Subclass like '%BTE%' 
                    then 'LEGAL EXPENSES BTE'

                    when 'LEGAL EXPENSES'  = coverage
                    AND   Subclass like '%ATE%' 
                    then 'LEGAL EXPENSES ATE'

                    when 'SURETY BONDS'  = coverage
                    then 'FINANCIAL INSTITUTIONS'
                    
                    else 'CGL'
                end

            when XFI_Product = 'DOWNSTREAM ENERGY'
            then
                case
                    when class in ('POWER GENERATION-CONVENTIONAL','POWER GENERATION-RENEWABLES')
                    then 'POWER & RENEWABLES'
                    else 'OIL & GAS'
                end

            when XFI_Product = 'ENGINEERING'
            then
                case
                    when coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
                    then 'IDI'
                    else 'ENGINEERING'
                end

            when XFI_Product = 'FORESTRY'
            then 'PROPERTY'

            when XFI_Product = 'MARINE'
            then 'MARINE CARGO'

            when XFI_Product = 'UPSTREAM ENERGY'
            then
                case
                    when class in ('CAR','EIO CAR')
                    then 'UPSTREAM CONSTRUCTION'
                    else 'UPSTREAM ENERGY'
                end

            when XFI_Product = 'MR'
            then 'INWARDS XOL'
        end as BudgetSegmentation_month,
	
	 case
            when XFI_Product in (
                'AVIATION','FINANCIAL INSTITUTIONS','INWARDS PPN','INWARDS XOL','MARINE LIABILITY',
                'MARINE TRADE','POLITICAL VIOLENCE','PORTS & TERMINALS','PROPERTY','CONTINGENCY'
            )
            then XFI_Product

            when XFI_Product = 'CASUALTY'
            then
                case
                    when 'PROFESSIONAL INDEMNITY' = coverage
                    and substring(PolicyReference,1,6) <> '600494'
                    then 'PROFESSIONAL INDEMNITY' 

                    when 'PROFESSIONAL INDEMNITY' = coverage
                    and substring(PolicyReference,1,6) = '600494'
                    then 'PEN' 

                    when REPLACE(coverage,'&amp;','&') in('DIRECTORS & OFFICERS','WARRANTY AND INDEMNITY' , 'PUBLIC OFFERING OF SECURITIES INSURANCE')
                    then 'DIRECTORS & OFFICERS'

                    when coverage in ('MEDICAL MALPRACTICE','SURETY BONDS')
                    then coverage

                    when 'LEGAL EXPENSES' = coverage
                    AND   Subclass like '%BTE%' 
                    then 'LEGAL EXPENSES BTE'

                    when 'LEGAL EXPENSES' = coverage
                    AND   Subclass like '%ATE%' 
                    then 'LEGAL EXPENSES ATE'

                    else 'CGL'
                end

            when XFI_Product = 'DOWNSTREAM ENERGY'
            then
                case
                    when class in ('POWER GENERATION-CONVENTIONAL','POWER GENERATION-RENEWABLES')
                    then 'POWER & RENEWABLES'
                    else 'OIL & GAS'
                end

            when XFI_Product = 'ENGINEERING'
            then
                case
                    when coverage in ('INHERENT DEFECTS INSURANCE','INSURANCE BACKED GUARANTEE','INHERENT DEFECTS INSURANCE - BI','INSURANCE BACKED GUARANTEE - BI')
                    then 'IDI'
                    else 'ENGINEERING'
                end

            when XFI_Product = 'FORESTRY'
            then 'PROPERTY'

            when XFI_Product = 'MARINE'
            then 'MARINE CARGO'

            when XFI_Product = 'UPSTREAM ENERGY'
            then
                case
                    when class in ('CAR','EIO CAR')
                    then 'UPSTREAM CONSTRUCTION'
                    else 'UPSTREAM ENERGY'
                end

            when XFI_Product = 'MR'
            then 'INWARDS XOL'
        end as SubClassSegmentation_month,

	class as class_month,

	producer_source as ProducerSource_month,

	producer as producer_month,

    ProducerGroup as ProducerGroup_month,

	'' as InsuranceType_month,

	-- map it with what we have in xfi
	Coverage as CoverageDesc_month,

	coverage_code as CoverageCode_month,

	Territory as territory_month,

	DomicileCountry as domicile_month,

	Region as region_month,
    region as region_split_month,

	-- from mar 2020 allcore
	MISUWY as MISUWY_month,
	
	UWY as UWY_month,

	ASSURED as insured_month,

	REASSURED as reassured_month,

	FILE_HANDLER as FileHandler_month,

	UNDERWRITER as underwriter_month,

	REC_UWR as RecUnderwriter_month,

	ADMIN_FILE_HANDLER as AdminFileHandler_month,

	'' as OperatorId_month,

	'' as activity_notes_period,

    PolicyInceptionDate as for_period_policy_inception,

    PolicyExpiryDate as for_period_policy_expiry,

   PolicyExpiryDate as for_period_policy_expiry_reported,

   '' as for_period_New_vs_Renwal

	FROM( 
        SELECT
            PolicyReference,
            PolicyInceptionDate,
            PolicyExpiryDate,
            IRIS_Product,
            XFI_Product,
            Ccy,
            ROE,
            Division,
            Subdivision,
            ProducerCode,
            Producer,
            ProducerGroupCode,
            ProducerGroup,
            ProducingOffice,
            Class,
            Coverage,
            Subclass,
            Territory,
            DomicileCountry,
            Region,
            Major_Region,
            GWP_Org as premium_org,
            GWP_AccCcy as premium_usd,
            ACQCOSTBOOKED_Org as acq_cost_org,
            ACQCOSTBOOKED_AccCcy as acq_cost_usd,
            --AGENCYFEESBOOKED_Org,
            --AGENCYFEESBOOKED_AccCcy as agency_fees_usd,
            EIOAGENCYFEESBOOKED_Org as eio_fees_org,
            EIOAGENCYFEESBOOKED_AccCcy as eio_fees_usd,
            ARGOFEESBOOKED_Org as argo_fees_org,
            ARGOFEESBOOKED_AccCcy as argo_fees_usd,
            AccountPeriod,
            MISUWY,
            UWY,
            ASSURED,
            REASSURED,
            FILE_HANDLER,
            UNDERWRITER,
            REC_UWR,
            ADMIN_FILE_HANDLER,
            coverage_code,
            producer_source
        FROM iris_mvmt
	) p

	UNPIVOT (apr_amount FOR type_of_monetary_amount in (premium_org, premium_usd, acq_cost_org, acq_cost_usd, eio_fees_org, eio_fees_usd, argo_fees_org, argo_fees_usd)) as unpvt

	WHERE apr_amount <> 0
    
)

SELECT * FROM iris_mvmts_reformated