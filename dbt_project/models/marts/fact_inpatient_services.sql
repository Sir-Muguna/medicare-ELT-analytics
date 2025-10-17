{{
    config(
        materialized='table'
    )
}}

with inpatient_2011 as (
    select
        -- Convert float to string and remove decimal part
        cast(cast(PROVIDER_ID as integer) as string) as provider_id,
        trim(HOSPITAL_REFERRAL_REGION_DESCRIPTION) as hospital_referral_region,
        
        -- Measures/Facts
        TOTAL_DISCHARGES as total_discharges,
        AVERAGE_COVERED_CHARGES as average_covered_charges,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_MEDICARE_PAYMENTS as average_medicare_payments,
        
        -- Diagnosis dimension key
        trim(ICD_CATEGORY) as icd_category,
        
        -- Time dimension
        2011 as service_year
    from {{ source('raw', 'inpatient_2011') }}
),

inpatient_2012 as (
    select
        -- Convert float to string and remove decimal part
        cast(cast(PROVIDER_ID as integer) as string) as provider_id,
        trim(HOSPITAL_REFERRAL_REGION_DESCRIPTION) as hospital_referral_region,
        TOTAL_DISCHARGES as total_discharges,
        AVERAGE_COVERED_CHARGES as average_covered_charges,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_MEDICARE_PAYMENTS as average_medicare_payments,
        trim(ICD_CATEGORY) as icd_category,
        2012 as service_year
    from {{ source('raw', 'inpatient_2012') }}
),

inpatient_2013 as (
    select
        -- Convert float to string and remove decimal part
        cast(cast(PROVIDER_ID as integer) as string) as provider_id,
        trim(HOSPITAL_REFERRAL_REGION_DESCRIPTION) as hospital_referral_region,
        TOTAL_DISCHARGES as total_discharges,
        AVERAGE_COVERED_CHARGES as average_covered_charges,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_MEDICARE_PAYMENTS as average_medicare_payments,
        trim(ICD_CATEGORY) as icd_category,
        2013 as service_year
    from {{ source('raw', 'inpatient_2013') }}
),

combined_inpatient as (
    select * from inpatient_2011
    union all
    select * from inpatient_2012
    union all
    select * from inpatient_2013
)

select 
    provider_id,
    hospital_referral_region,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments,
    icd_category,
    service_year
from combined_inpatient