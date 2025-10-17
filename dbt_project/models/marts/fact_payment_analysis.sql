{{
    config(
        materialized='table'
    )
}}

with inpatient_payments as (
    select
        -- Convert float to string and remove decimal part
        cast(cast(provider_id as integer) as string) as provider_id,
        cast(service_year as integer) as service_year,
        'inpatient' as care_setting,
        cast(total_discharges as integer) as service_volume,
        cast(average_medicare_payments as numeric) as medicare_payment_amount,
        cast(average_total_payments as numeric) as total_payment_amount,
        cast(average_covered_charges as numeric) as submitted_charges_amount
    from {{ ref('stg_inpatient_charges') }}
),

outpatient_payments as (
    select
        -- Convert float to string and remove decimal part
        cast(cast(provider_id as integer) as string) as provider_id,
        cast(service_year as integer) as service_year,
        'outpatient' as care_setting,
        cast(outpatient_services_count as integer) as service_volume,
        cast(average_total_payments as numeric) as medicare_payment_amount,
        cast(average_total_payments as numeric) as total_payment_amount,
        cast(average_submitted_charges as numeric) as submitted_charges_amount
    from {{ ref('stg_outpatient_charges') }}
),

combined_payments as (
    select 
        provider_id,
        service_year,
        care_setting,
        service_volume,
        medicare_payment_amount,
        total_payment_amount,
        submitted_charges_amount
    from inpatient_payments
    union all
    select 
        provider_id,
        service_year,
        care_setting,
        service_volume,
        medicare_payment_amount,
        total_payment_amount,
        submitted_charges_amount
    from outpatient_payments
),

-- Aggregate to ensure uniqueness at the grain level
aggregated_payments as (
    select
        provider_id,
        service_year,
        care_setting,
        sum(service_volume) as service_volume,
        avg(medicare_payment_amount) as medicare_payment_amount,
        avg(total_payment_amount) as total_payment_amount,
        avg(submitted_charges_amount) as submitted_charges_amount
    from combined_payments
    group by provider_id, service_year, care_setting
),

payment_analysis as (
    select
        -- Ensure unique surrogate key at the correct grain
        md5(
            provider_id || '-' || 
            cast(service_year as string) || '-' || 
            care_setting
        ) as payment_analysis_key,
        
        -- Foreign Keys (now properly formatted as string without decimals)
        provider_id,
        service_year,
        care_setting,
        
        -- Measures
        service_volume,
        medicare_payment_amount,
        total_payment_amount,
        submitted_charges_amount,
        
        -- Derived metrics
        round(medicare_payment_amount / nullif(submitted_charges_amount, 0), 4) as medicare_payment_ratio
    from aggregated_payments
)

select * from payment_analysis