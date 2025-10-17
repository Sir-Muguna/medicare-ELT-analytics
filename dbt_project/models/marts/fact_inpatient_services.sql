{{
    config(
        materialized='table',
        cluster_by=['service_year', 'provider_id']
    )
}}

with inpatient_base as (
    select
        cast(service_year as integer) as service_year,
        upper(cast(icd_category as string)) as diagnosis_code,
        cast(cast(provider_id as integer) as string) as provider_id,
        cast(total_discharges as integer) as total_discharges,
        cast(average_covered_charges as numeric) as average_covered_charges,
        cast(average_total_payments as numeric) as average_total_payments,
        cast(average_medicare_payments as numeric) as average_medicare_payments
    from {{ ref('stg_inpatient_charges') }}
    where provider_id is not null 
      and service_year is not null 
      and icd_category is not null
),

final_fact_inpatient as (
    select
        -- Surrogate Key for fact table
        md5(
            cast(ib.provider_id as string) || '-' || 
            cast(ib.service_year as string) || '-' || 
            cast(ib.diagnosis_code as string)  
        ) as inpatient_id,
        
        -- Foreign Keys to Dimensions
        ib.provider_id,
        ib.diagnosis_code,
        ib.service_year,
        
        -- Measures/Facts
        ib.total_discharges,
        ib.average_covered_charges,
        ib.average_total_payments,
        ib.average_medicare_payments,
        
        -- Derived metrics (calculated facts)
        round(ib.average_medicare_payments / nullif(ib.average_covered_charges, 0), 2) as medicare_payment_ratio
    from inpatient_base ib
)

select * from final_fact_inpatient