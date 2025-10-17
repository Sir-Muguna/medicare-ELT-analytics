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
        split_part(trim(APC), ' - ', 1) as apc_code,
        split_part(trim(APC), ' - ', 2) as apc_description,
        case 
            when try_cast(trim(PROVIDER_ID) as integer) is not null then 
                cast(cast(trim(PROVIDER_ID) as integer) as string)
            else trim(PROVIDER_ID)
        end as provider_id,
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
        split_part(trim(APC), ' - ', 1) as apc_code,
        split_part(trim(APC), ' - ', 2) as apc_description,
        case 
            when try_cast(trim(PROVIDER_ID) as integer) is not null then 
                cast(cast(trim(PROVIDER_ID) as integer) as string)
            else trim(PROVIDER_ID)
        end as provider_id,
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
        split_part(trim(APC), ' - ', 1) as apc_code,
        split_part(trim(APC), ' - ', 2) as apc_description,
        case 
            when try_cast(trim(PROVIDER_ID) as integer) is not null then 
                cast(cast(trim(PROVIDER_ID) as integer) as string)
            else trim(PROVIDER_ID)
        end as provider_id,
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

select 
    _AIRBYTE_RAW_ID,
    _AIRBYTE_EXTRACTED_AT,
    apc_code,
    apc_description,
    provider_id,           
    provider_name,
    provider_street_address,
    provider_city,
    provider_state,
    provider_zipcode,
    outpatient_services_count,
    average_total_payments,
    average_submitted_charges,
    service_year
from combined_outpatient