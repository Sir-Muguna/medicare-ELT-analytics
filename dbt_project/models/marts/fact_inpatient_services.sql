{{
    config(
        materialized='table',
        cluster_by=['service_year', 'provider_state']
    )
}}

with inpatient_base as (
    select
        cast(service_year as integer) as service_year,
        upper(cast(icd_category as string)) as icd_category,
        -- Remove decimal and cast to string to match dim_providers format
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

provider_context as (
    select
        provider_id,
        upper(cast(provider_name as string)) as provider_name,
        upper(cast(provider_state as string)) as provider_state,
        upper(cast(provider_type as string)) as provider_type,
        offers_emergency_services,
        quality_rating
    from {{ ref('dim_providers') }}
),

diagnosis_context as (
    select
        upper(cast(diagnosis_code as string)) as diagnosis_code,
        upper(cast(diagnosis_description as string)) as diagnosis_description,
        upper(cast(chapter as string)) as chapter,
        upper(cast(chapter_description as string)) as chapter_description,
        upper(cast(code_category as string)) as code_category
    from {{ ref('dim_diagnosis_codes') }}
    where code_system = 'ICD-10'
),

combined_data as (
    select
        ib.service_year,
        ib.icd_category,
        ib.provider_id,
        pc.provider_name,
        pc.provider_state,
        pc.provider_type,
        pc.offers_emergency_services,
        pc.quality_rating,
        dc.diagnosis_description,
        dc.chapter,
        dc.chapter_description,
        dc.code_category,
        ib.total_discharges,
        ib.average_covered_charges,
        ib.average_total_payments,
        ib.average_medicare_payments,
        -- Derived metrics
        round(ib.average_medicare_payments / nullif(ib.average_covered_charges, 0), 4) as medicare_payment_ratio,
        -- Volume categorization
        case
            when ib.total_discharges > 1000 then 'HIGH_VOLUME'
            when ib.total_discharges >= 100 then 'MEDIUM_VOLUME'
            else 'LOW_VOLUME'
        end as discharge_volume_category
    from inpatient_base ib
    inner join provider_context pc on ib.provider_id = pc.provider_id
    left join diagnosis_context dc on ib.icd_category = dc.diagnosis_code
)

select
    -- UUID Primary Key
    md5(
        cast(provider_id as string) || '-' || 
        cast(service_year as string) || '-' || 
        cast(icd_category as string)
    ) as inpatient_id,
    service_year,
    provider_id,
    provider_name,
    provider_state,
    provider_type,
    offers_emergency_services,
    quality_rating,
    chapter,
    chapter_description,
    code_category,
    icd_category,
    diagnosis_description,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments,
    medicare_payment_ratio,
    discharge_volume_category
from combined_data