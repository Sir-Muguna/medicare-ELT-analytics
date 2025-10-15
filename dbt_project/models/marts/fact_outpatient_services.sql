{{
    config(
        materialized='table',
        cluster_by=['service_year', 'provider_state']
    )
}}

with outpatient_base as (
    select
        cast(service_year as integer) as service_year,
        -- Split APC code and description
        split_part(apc, ' - ', 1) as apc_code_raw,
        split_part(apc, ' - ', 2) as apc_description_raw,
        cast(provider_id as string) as provider_id,
        cast(outpatient_services_count as integer) as service_volume,
        cast(average_total_payments as numeric) as average_payment_amount,
        cast(average_submitted_charges as numeric) as average_submitted_charge
    from {{ ref('stg_outpatient_charges') }}
    where provider_id is not null 
      and service_year is not null 
      and apc is not null
),

apc_split as (
    select
        service_year,
        upper(trim(apc_code_raw)) as apc_code,
        upper(trim(apc_description_raw)) as apc_description,
        provider_id,
        service_volume,
        average_payment_amount,
        average_submitted_charge
    from outpatient_base
    where apc_code_raw != '' and apc_description_raw != ''
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

combined_data as (
    select
        apc.service_year,
        apc.apc_code,
        apc.apc_description,
        apc.provider_id,
        pc.provider_name,
        pc.provider_state,
        pc.provider_type,
        pc.offers_emergency_services,
        pc.quality_rating,
        apc.service_volume,
        apc.average_payment_amount,
        apc.average_submitted_charge,
        -- Derived metrics
        round(apc.average_payment_amount / nullif(apc.average_submitted_charge, 0), 4) as payment_to_charge_ratio,
        -- Volume categorization
        case
            when apc.service_volume > 10000 then 'HIGH_VOLUME'
            when apc.service_volume >= 1000 then 'MEDIUM_VOLUME'
            else 'LOW_VOLUME'
        end as service_volume_category
    from apc_split apc
    inner join provider_context pc on apc.provider_id = pc.provider_id
)


select
    -- UUID Primary Key
    md5(
        cast(provider_id as string) || '-' || 
        cast(service_year as string) || '-' || 
        cast(apc_code as string)
    ) as outpatient_id,
    service_year,
    apc_code,
    apc_description,
    provider_id,
    provider_name,
    provider_state,
    provider_type,
    offers_emergency_services,
    quality_rating,
    service_volume,
    average_payment_amount,
    average_submitted_charge,
    payment_to_charge_ratio,
    service_volume_category
from combined_data