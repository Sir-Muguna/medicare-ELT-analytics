{{
    config(
        materialized='table',
        cluster_by=['provider_state', 'provider_type']
    )
}}

with provider_base as (
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

referral_regions as (
    select distinct
        cast(provider_id as string) as provider_id,
        cast(hospital_referral_region as string) as referral_region
    from {{ ref('stg_inpatient_charges') }}
)

select
    pb.provider_id,
    pb.provider_name,
    pb.provider_state,
    pb.county_name,
    pb.city,
    pb.zip_code,
    pb.provider_type,
    pb.ownership_type,
    pb.offers_emergency_services,
    pb.ehr_interoperable,
    pb.quality_rating,
    pb.safety_measures_count,
    pb.readmission_measures_count,
    -- rr.referral_region,
    -- Derived business logic with uppercase values
    case
        when pb.offers_emergency_services and pb.quality_rating >= 4 then 'PREMIUM'
        when pb.offers_emergency_services then 'STANDARD_EMERGENCY' 
        when pb.quality_rating >= 4 then 'HIGH_QUALITY'
        else 'BASIC'
    end as service_level
from provider_base pb
left join referral_regions rr on pb.provider_id = rr.provider_id