{{
    config(
        materialized='table',
        cluster_by=['provider_state', 'provider_type']
    )
}}

with hospital_providers as (
    select
        cast(provider_id as string) as provider_id,
        upper(cast(hospital_name as string)) as provider_name,
        upper(cast(state as string)) as provider_state,
        upper(cast(county_name as string)) as county_name,
        upper(cast(city as string)) as city,
        cast(zip_code as string) as zip_code,
        upper(cast(hospital_type as string)) as provider_type,
        upper(cast(hospital_ownership as string)) as ownership_type,
        cast(has_emergency_services as boolean) as offers_emergency_services,
        cast(meets_ehr_interoperability_criteria as boolean) as ehr_interoperable,
        cast(hospital_overall_rating as integer) as quality_rating,
        cast(safety_measures_count as integer) as safety_measures_count,
        cast(readmission_measures_count as integer) as readmission_measures_count
    from {{ ref('stg_hospital_general_info') }}
),

-- Debug: Check what provider_ids exist in inpatient that don't match
inpatient_providers as (
    select distinct 
        cast(provider_id as string) as provider_id,
        cast(provider_id as string) as original_provider_id
    from {{ ref('stg_inpatient_charges') }}
),

outpatient_providers as (
    select distinct cast(provider_id as string) as provider_id
    from {{ ref('stg_outpatient_charges') }}
),

-- Get ALL providers with proper casting
all_provider_ids as (
    select provider_id from inpatient_providers
    union
    select provider_id from outpatient_providers
),

-- Enrich with available provider data
enriched_providers as (
    select
        api.provider_id,
        -- Provider attributes (with fallbacks for missing data)
        coalesce(hp.provider_name, 'UNKNOWN_PROVIDER') as provider_name,
        coalesce(hp.provider_state, 'UNKNOWN_STATE') as provider_state,
        hp.county_name,
        hp.city,
        hp.zip_code,
        coalesce(hp.provider_type, 'UNKNOWN_TYPE') as provider_type,
        hp.ownership_type,
        coalesce(hp.offers_emergency_services, false) as offers_emergency_services,
        coalesce(hp.ehr_interoperable, false) as ehr_interoperable,
        hp.quality_rating,
        hp.safety_measures_count,
        hp.readmission_measures_count
    from all_provider_ids api
    left join hospital_providers hp on api.provider_id = hp.provider_id
),

final_providers as (
    select
        -- Natural Key
        provider_id,
        
        -- Surrogate Key
        md5(
            coalesce(provider_id, '') ||
            coalesce(provider_name, '') ||
            coalesce(provider_state, '') ||
            coalesce(provider_type, '')
        ) as provider_key,
        
        -- Provider Attributes
        provider_name,
        provider_state,
        county_name,
        city,
        zip_code,
        provider_type,
        ownership_type,
        offers_emergency_services,
        ehr_interoperable,
        quality_rating,
        safety_measures_count,
        readmission_measures_count,
        
        -- Derived Business Logic
        case
            when offers_emergency_services and quality_rating >= 4 then 'PREMIUM'
            when offers_emergency_services then 'STANDARD_EMERGENCY' 
            when quality_rating >= 4 then 'HIGH_QUALITY'
            else 'BASIC'
        end as service_level,
        
        -- SCD Type 2 Metadata
        current_timestamp() as effective_date,
        null as end_date,
        true as is_current_flag
        
    from enriched_providers
)

select * from final_providers