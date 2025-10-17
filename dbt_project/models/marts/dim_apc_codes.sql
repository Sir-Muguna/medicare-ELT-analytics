-- models/marts/dim_apc_codes.sql
{{
    config(
        materialized='table',
        cluster_by=['apc_code']
    )
}}

with unique_apc_codes as (
    select distinct
        upper(cast(trim(apc_code) as string)) as apc_code,
        upper(trim(apc_description)) as apc_description
    from {{ ref('stg_outpatient_charges') }}
    where apc_code is not null 
      and apc_code != ''
),

final_apc_codes as (
    select
        -- Surrogate Key
        md5(apc_code) as apc_id,
        
        -- Natural Key
        apc_code,
        
        -- Descriptive Attributes
        apc_description,
        
        -- Metadata
        current_timestamp() as effective_date,
        null as end_date,
        true as is_current_flag
        
    from unique_apc_codes
)

select * from final_apc_codes