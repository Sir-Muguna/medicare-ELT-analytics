-- models/staging/stg_medicare_data.sql
{{
    config(
        materialized='view'
    )
}}

with raw_data as (
    select
        -- Airbyte system columns (kept for audit)
        _airbyte_raw_id,
        _airbyte_extracted_at,
        
        -- Drug identification
        md5(trim(brnd_name) || '|' || trim(gnrc_name)) as drug_key,
        trim(brnd_name) as brnd_name,
        trim(gnrc_name) as gnrc_name,
        
        -- Prescription volume metrics (converted from string to proper types)
        nullif(try_cast(tot_clms as integer), '') as tot_clms,
        nullif(try_cast(tot_benes as integer), '') as tot_benes,
        nullif(try_cast(tot_prscrbrs as integer), '') as tot_prscrbrs,
        nullif(try_cast(tot_30day_fills as float), '') as tot_30day_fills,
        
        -- Cost metrics (converted from string to float)
        nullif(try_cast(tot_drug_cst as float), '') as tot_drug_cst,
        nullif(try_cast(lis_bene_cst_shr as float), '') as lis_bene_cst_shr,
        nullif(try_cast(nonlis_bene_cst_shr as float), '') as nonlis_bene_cst_shr,
        
        -- Senior population (65+) metrics
        nullif(try_cast(ge65_tot_clms as integer), '') as ge65_tot_clms,
        nullif(try_cast(ge65_tot_benes as integer), '') as ge65_tot_benes,
        nullif(try_cast(ge65_tot_30day_fills as float), '') as ge65_tot_30day_fills,
        nullif(try_cast(ge65_tot_drug_cst as float), '') as ge65_tot_drug_cst,
        
        -- Drug category flags (standardized to boolean)
        case 
            when antbtc_drug_flag = 'Y' then true
            when antbtc_drug_flag = 'N' then false
            else null
        end as is_antibiotic_drug,
        
        case 
            when opioid_drug_flag = 'Y' then true
            when opioid_drug_flag = 'N' then false
            else null
        end as is_opioid_drug,
        
        case 
            when opioid_la_drug_flag = 'Y' then true
            when opioid_la_drug_flag = 'N' then false
            else null
        end as is_long_acting_opioid,
        
        case 
            when antpsyct_drug_flag = 'Y' then true
            when antpsyct_drug_flag = 'N' then false
            else null
        end as is_antipsychotic_drug,
        
        -- Suppression flags (standardized to boolean with null handling)
        case 
            when ge65_sprsn_flag = 'Y' then true
            when ge65_sprsn_flag = 'N' then false
            else null
        end as is_ge65_suppressed,
        
        case 
            when ge65_bene_sprsn_flag = 'Y' then true
            when ge65_bene_sprsn_flag = 'N' then false
            else null
        end as is_ge65_beneficiary_suppressed,
        
        -- Geographic information (hardcoded based on EDA findings)
        'NATIONAL' as prescriber_geo_level,
        'NATIONAL' as prescriber_geo_description
        
    from {{ source('raw', 'medicare_data') }}
)

select * from raw_data