{{
    config(
        materialized='table',
        cluster_by=['code_system', 'chapter']
    )
}}

with icd_10_codes as (
    select
        upper(cast(icd_code as string)) as diagnosis_code,
        'ICD-10' as code_system,
        upper(cast(disease_description as string)) as diagnosis_description,
        upper(cast(chapter as string)) as chapter,
        upper(cast(chapter_description as string)) as chapter_description,
        upper(cast(code_category as string)) as code_category,
        case 
            when icd_code = code_category then 2  -- Category level
            else 3  -- Individual code level
        end as hierarchy_level,
        true as is_leaf_code,
        'STANDARD' as classification_type,
        false as is_residual_category
    from {{ ref('stg_icd_10_codes') }}
    where icd_code is not null
),

icd_11_codes as (
    select
        upper(cast(icd_code as string)) as diagnosis_code,
        'ICD-11' as code_system,
        upper(cast(disease_title as string)) as diagnosis_description,
        upper(cast(chapter_number as string)) as chapter,
        'CHAPTER_' || cast(chapter_number as string) as chapter_description,
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
),

final_diagnosis as (
    select
        -- Surrogate Key
        md5(diagnosis_code || code_system) as diagnosis_key,
        -- Natural Key
        diagnosis_code,
        code_system,
        -- Descriptive Attributes
        diagnosis_description,
        chapter,
        chapter_description,
        code_category,
        hierarchy_level,
        classification_type,
        is_leaf_code,
        is_residual_category
    from unified_codes
    where diagnosis_code is not null
)

select * from final_diagnosis