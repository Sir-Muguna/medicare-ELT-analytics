-- models/staging/stg_icd_10_codes.sql
{{
    config(
        materialized='view'
    )
}}

with icd_10_raw_data as (
    select
        -- Airbyte system columns (kept for audit)
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        
        -- ICD-10 Classification Hierarchy
        trim(CHAPTER) as chapter,
        trim(CHAPTER_DESCRIPTION) as chapter_description,
        trim(CODE_CATEGORY) as code_category,
        trim(CODE_CATEGORY_DESCRIPTION) as code_category_description,
        
        -- Individual disease codes with NULL handling
        case 
            when ICD_CODE is not null and ICD_CODE != '' 
            then trim(ICD_CODE) 
            else null 
        end as icd_code,
        
        case 
            when DISEASE_DESCRIPTION is not null and DISEASE_DESCRIPTION != '' 
            then trim(DISEASE_DESCRIPTION) 
            else null 
        end as disease_description
        
    from {{ source('raw', 'icd_10') }}
)

select * from icd_10_raw_data