{{
    config(
        materialized='table'
    )
}}

with inpatient_payments as (
    select
        cast(provider_id as string) as provider_id,
        upper(cast(provider_name as string)) as provider_name,
        upper(cast(provider_state as string)) as provider_state,
        cast(service_year as integer) as service_year,
        upper('inpatient') as care_setting,
        cast(total_discharges as integer) as service_volume,
        cast(average_medicare_payments as numeric) as medicare_payment_amount,
        cast(average_total_payments as numeric) as total_payment_amount,
        cast(average_covered_charges as numeric) as submitted_charges_amount
    from {{ ref('stg_inpatient_charges') }}
),

outpatient_payments as (
    select
        cast(provider_id as string) as provider_id,
        upper(cast(provider_name as string)) as provider_name, 
        upper(cast(provider_state as string)) as provider_state,
        cast(service_year as integer) as service_year,
        upper('outpatient') as care_setting,
        cast(outpatient_services_count as integer) as service_volume,
        cast(average_total_payments as numeric) as medicare_payment_amount,
        cast(average_total_payments as numeric) as total_payment_amount,
        cast(average_submitted_charges as numeric) as submitted_charges_amount
    from {{ ref('stg_outpatient_charges') }}
),

combined_payments as (
    select * from inpatient_payments
    union all
    select * from outpatient_payments
)

select
    cast(provider_id as string) as provider_id,
    cast(provider_name as string) as provider_name,
    cast(provider_state as string) as provider_state,
    cast(service_year as integer) as service_year,
    cast(care_setting as string) as care_setting,
    cast(service_volume as integer) as service_volume,
    cast(medicare_payment_amount as numeric) as medicare_payment_amount,
    cast(total_payment_amount as numeric) as total_payment_amount,
    cast(submitted_charges_amount as numeric) as submitted_charges_amount,
    -- Derived metrics
    cast(round(medicare_payment_amount / nullif(submitted_charges_amount, 0), 4) as numeric) as medicare_payment_ratio,
    cast(
        case 
            when service_volume > 1000 then 'high_volume'
            when service_volume > 100 then 'medium_volume' 
            else 'low_volume'
        end as string
    ) as volume_category
from combined_payments