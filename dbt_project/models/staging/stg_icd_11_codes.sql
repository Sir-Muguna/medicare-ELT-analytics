-- models/staging/stg_icd_11_codes.sql
{{
    config(
        materialized='view'
    )
}}

with icd_11_raw_data as (
    select
        -- Airbyte system columns (kept for audit)
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        
        -- ICD-11 Core Classification Data
        case 
            when CODE is not null and CODE != '' 
            then trim(CODE) 
            else null 
        end as icd_code,
        
        trim(TITLE) as disease_title,
        
        -- Boolean flags conversion
        case 
            when ISLEAF = 'TRUE' then true
            when ISLEAF = 'FALSE' then false
            else null
        end as is_leaf_node,
        
        case 
            when ISRESIDUAL = 'TRUE' then true
            when ISRESIDUAL = 'FALSE' then false
            else null
        end as is_residual_category,
        
        case 
            when "PRIMARY TABULATION" = 'TRUE' then true
            when "PRIMARY TABULATION" = 'FALSE' then false
            else null
        end as is_primary_tabulation,
        
        -- Numeric conversions
        try_cast(CHAPTERNO as integer) as chapter_number,
        try_cast(DEPTHINKIND as integer) as hierarchy_depth,
        
        -- Classification metadata
        trim(CLASSKIND) as classification_kind,
        
        -- Grouping categories (with null handling)
        case 
            when GROUPING1 is not null and GROUPING1 != '' 
            then trim(GROUPING1) 
            else null 
        end as primary_grouping,
        
        case 
            when GROUPING2 is not null and GROUPING2 != '' 
            then trim(GROUPING2) 
            else null 
        end as secondary_grouping,
        
        -- URI references
        case 
            when "FOUNDATION URI" is not null and "FOUNDATION URI" != '' 
            then trim("FOUNDATION URI") 
            else null 
        end as foundation_ontology_uri,
        
        trim("LINEARIZATION URI") as linearization_uri
        
    from {{ source('raw', 'icd_11') }}
)

select * from icd_11_raw_data