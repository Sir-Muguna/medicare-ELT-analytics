-- models/staging/stg_outpatient_charges.sql
{{
    config(
        materialized='view'
    )
}}

with outpatient_2011 as (
    select
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        trim(APC) as apc,
        trim(PROVIDER_ID) as provider_id,
        trim(PROVIDER_NAME) as provider_name,
        trim(PROVIDER_STREET_ADDRESS) as provider_street_address,
        trim(PROVIDER_CITY) as provider_city,
        trim(PROVIDER_STATE) as provider_state,
        cast(PROVIDER_ZIPCODE as string) as provider_zipcode,
        OUTPATIENT_SERVICES as outpatient_services_count,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_ESTIMATED_SUBMITTED_CHARGES as average_submitted_charges,
        2011 as service_year
    from {{ source('raw', 'outpatient_charges_2011') }}
),

outpatient_2012 as (
    select
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        trim(APC) as apc,
        trim(PROVIDER_ID) as provider_id,
        trim(PROVIDER_NAME) as provider_name,
        trim(PROVIDER_STREET_ADDRESS) as provider_street_address,
        trim(PROVIDER_CITY) as provider_city,
        trim(PROVIDER_STATE) as provider_state,
        cast(PROVIDER_ZIPCODE as string) as provider_zipcode,
        OUTPATIENT_SERVICES as outpatient_services_count,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_ESTIMATED_SUBMITTED_CHARGES as average_submitted_charges,
        2012 as service_year
    from {{ source('raw', 'outpatient_charges_2012') }}
),

outpatient_2013 as (
    select
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        trim(APC) as apc,
        trim(PROVIDER_ID) as provider_id,
        trim(PROVIDER_NAME) as provider_name,
        trim(PROVIDER_STREET_ADDRESS) as provider_street_address,
        trim(PROVIDER_CITY) as provider_city,
        trim(PROVIDER_STATE) as provider_state,
        cast(PROVIDER_ZIPCODE as string) as provider_zipcode,
        OUTPATIENT_SERVICES as outpatient_services_count,
        AVERAGE_TOTAL_PAYMENTS as average_total_payments,
        AVERAGE_ESTIMATED_SUBMITTED_CHARGES as average_submitted_charges,
        2013 as service_year
    from {{ source('raw', 'outpatient_charges_2013') }}
),

combined_outpatient as (
    select * from outpatient_2011
    union all
    select * from outpatient_2012
    union all
    select * from outpatient_2013
)

select * from combined_outpatient