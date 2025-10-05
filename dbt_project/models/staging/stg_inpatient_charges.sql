-- models/staging/stg_inpatient_charges.sql
{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with inpatient_2011 as (
    select
        trim(PROVIDER_ID) as provider_id,
        trim(PROVIDER_NAME) as provider_name,
        trim(PROVIDER_STREET_ADDRESS) as provider_street_address,
        trim(PROVIDER_CITY) as provider_city,
        trim(PROVIDER_STATE) as provider_state,
        cast(PROVIDER_ZIPCODE as string) as provider_zipcode,
        trim(HOSPITAL_REFERRAL_REGION_DESCRIPTION) as hospital_referral_region,
        TOTAL_DISCHARGES as total_discharges,
        AVERAGE_COVERED_CHARGES as average_covered_charges,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_MEDICARE_PAYMENTS as average_medicare_payments,
        trim(ICD_CATEGORY) as icd_category,
        2011 as service_year
    from {{ source('raw', 'inpatient_2011') }}
),

inpatient_2012 as (
    select
        trim(PROVIDER_ID) as provider_id,
        trim(PROVIDER_NAME) as provider_name,
        trim(PROVIDER_STREET_ADDRESS) as provider_street_address,
        trim(PROVIDER_CITY) as provider_city,
        trim(PROVIDER_STATE) as provider_state,
        cast(PROVIDER_ZIPCODE as string) as provider_zipcode,
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
        trim(PROVIDER_ID) as provider_id,
        trim(PROVIDER_NAME) as provider_name,
        trim(PROVIDER_STREET_ADDRESS) as provider_street_address,
        trim(PROVIDER_CITY) as provider_city,
        trim(PROVIDER_STATE) as provider_state,
        cast(PROVIDER_ZIPCODE as string) as provider_zipcode,
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

select * from combined_inpatient