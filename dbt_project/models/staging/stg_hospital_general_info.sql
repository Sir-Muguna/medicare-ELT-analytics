-- models/staging/stg_hospitals.sql
{{
    config(
        materialized='view'
    )
}}

with hospital_info_raw_data as (
    select
        -- Airbyte system columns (kept for audit)
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        
        -- Hospital Identification & Location
        trim(PROVIDER_ID) as provider_id,
        trim(HOSPITAL_NAME) as hospital_name,
        trim(ADDRESS) as address,
        trim(CITY) as city,
        trim(STATE) as state,
        trim(ZIP_CODE) as zip_code,
        trim(COUNTY_NAME) as county_name,
        trim(PHONE_NUMBER) as phone_number,
        
        -- Hospital Characteristics
        trim(HOSPITAL_TYPE) as hospital_type,
        trim(HOSPITAL_OWNERSHIP) as hospital_ownership,
        
        -- Boolean conversions
        case 
            when EMERGENCY_SERVICES = 'Yes' then true
            else false
        end as has_emergency_services,
        
        case 
            when MEETS_CRITERIA_FOR_PROMOTING_INTEROPERABILITY_OF_EHRS is not null 
            then true 
            else false
        end as meets_ehr_interoperability_criteria,
        
        -- Quality Ratings (numeric conversions)
        try_cast(HOSPITAL_OVERALL_RATING as integer) as hospital_overall_rating,
        
        -- Measure Counts (numeric conversions)
        try_cast(SAFETY_MEASURES_COUNT as integer) as safety_measures_count,
        try_cast(READMISSION_MEASURES_COUNT as integer) as readmission_measures_count,
        try_cast(SAFETY_MEASURES_WORSE_COUNT as integer) as safety_measures_worse_count,
        try_cast(SAFETY_MEASURES_BETTER_COUNT as integer) as safety_measures_better_count,
        try_cast(MORTALITY_GROUP_MEASURE_COUNT as integer) as mortality_group_measure_count,
        try_cast(MORTALITY_MEASURES_WORSE_COUNT as integer) as mortality_measures_worse_count,
        try_cast(MORTALITY_MEASURES_BETTER_COUNT as integer) as mortality_measures_better_count,
        try_cast(READMISSION_MEASURES_WORSE_COUNT as integer) as readmission_measures_worse_count,
        try_cast(PATIENT_EXPERIENCE_MEASURES_COUNT as integer) as patient_experience_measures_count,
        try_cast(READMISSION_MEASURES_BETTER_COUNT as integer) as readmission_measures_better_count,
        try_cast(FACILITY_MORTAILITY_MEASURES_COUNT as integer) as facility_mortality_measures_count,
        try_cast(SAFETY_MEASURES_NO_DIFFERENT_COUNT as integer) as safety_measures_no_different_count,
        try_cast(FACILITY_CARE_SAFETY_MEASURES_COUNT as integer) as facility_care_safety_measures_count,
        try_cast(FACILITY_READMISSION_MEASURES_COUNT as integer) as facility_readmission_measures_count,
        try_cast(MORTALITY_MEASURES_NO_DIFFERENT_COUNT as integer) as mortality_measures_no_different_count,
        try_cast(READMISSION_MEASURES_NO_DIFFERENT_COUNT as integer) as readmission_measures_no_different_count,
        try_cast(TIMELY_AND_EFFECTIVE_CARE_MEASURES_COUNT as integer) as timely_care_measures_count,
        try_cast(FACILITY_PATIENT_EXPERIENCE_MEASURES_COUNT as integer) as facility_patient_experience_measures_count,
        try_cast(FACILITY_TIMELY_AND_EFFECTIVE_CARE_MEASURES_COUNT as integer) as facility_timely_care_measures_count,
        
        -- Footnotes (with null handling)
        case 
            when HOSPITAL_OVERALL_RATING_FOOTNOTE is not null and HOSPITAL_OVERALL_RATING_FOOTNOTE != '' 
            then trim(HOSPITAL_OVERALL_RATING_FOOTNOTE) 
            else null 
        end as hospital_overall_rating_footnote,
        
        case 
            when PATIENT_EXPERIENCE_MEASURES_FOOTNOTE is not null and PATIENT_EXPERIENCE_MEASURES_FOOTNOTE != '' 
            then trim(PATIENT_EXPERIENCE_MEASURES_FOOTNOTE) 
            else null 
        end as patient_experience_measures_footnote
        
    from {{ source('raw', 'hospital_general_info') }}
)

select * from hospital_info_raw_data