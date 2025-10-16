{{
    config(
        materialized='table',
        cluster_by=['code_system', 'chapter']
    )
}}

with icd_10_codes as (
    select
        upper(cast(icd_code as string)) as icd_code,  
        'ICD-10' as code_system,
        upper(cast(disease_description as string)) as diagnosis_description,
        upper(cast(chapter as string)) as chapter,
        upper(cast(chapter_description as string)) as chapter_description,
        upper(cast(code_category as string)) as code_category,
        -- Derive hierarchy level for ICD-10 (chapter=1, category=2, individual=3)
        case 
            when icd_code = code_category then 2  -- Category level
            else 3  -- Individual code level
        end as hierarchy_level,
        true as is_leaf_code,  -- ICD-10 codes are typically leaf nodes
        upper(cast('STANDARD' as string)) as classification_type,
        false as is_residual_category
    from {{ ref('stg_icd_10_codes') }}
    where icd_code is not null
),

icd_11_codes as (
    select
        upper(cast(icd_code as string)) as icd_code,  
        'ICD-11' as code_system,
        upper(cast(disease_title as string)) as diagnosis_description,
        upper(cast(chapter_number as string)) as chapter,
        upper(cast('CHAPTER_' || cast(chapter_number as string) as string)) as chapter_description,
        upper(cast(primary_grouping as string)) as code_category,
        cast(hierarchy_depth as integer) as hierarchy_level,
        cast(is_leaf_node as boolean) as is_leaf_code,
        upper(cast(classification_kind as string)) as classification_type,
        cast(is_residual_category as boolean) as is_residual_category
    from {{ ref('stg_icd_11_codes') }}
    where icd_code is not null
),

unified_codes as (
    select * from icd_10_codes
    union all
    select * from icd_11_codes
)

select
    icd_code,  
    code_system,
    code_category,
    diagnosis_description,
    chapter,
    chapter_description,
    hierarchy_level,
    classification_type
from unified_codes
where icd_code is not null  