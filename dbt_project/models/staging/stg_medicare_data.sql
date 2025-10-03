-- models/staging/stg_medicare_data.sql
{{
    config(
        materialized='view'
    )
}}

with raw_medicare_data as (
    select
        -- Airbyte system columns (kept for audit)
        _airbyte_raw_id,
        _airbyte_extracted_at,
        
        -- Drug identification
        md5(trim(brnd_name) || '|' || trim(gnrc_name)) as drug_id,
        trim(brnd_name) as brnd_name,
        trim(gnrc_name) as gnrc_name,
        
        -- Prescription volume metrics (with better empty string handling)
        case when nullif(trim(tot_clms), '') is not null 
             then try_cast(tot_clms as integer) 
             else null 
        end as tot_clms,
        
        case when nullif(trim(tot_benes), '') is not null 
             then try_cast(tot_benes as integer) 
             else null 
        end as tot_benes,
        
        case when nullif(trim(tot_prscrbrs), '') is not null 
             then try_cast(tot_prscrbrs as integer) 
             else null 
        end as tot_prscrbrs,
        
        case when nullif(trim(tot_30day_fills), '') is not null 
             then try_cast(tot_30day_fills as float) 
             else null 
        end as tot_30day_fills,
        
        -- Cost metrics
        case when nullif(trim(tot_drug_cst), '') is not null 
             then try_cast(tot_drug_cst as float) 
             else null 
        end as tot_drug_cst,
        
        case when nullif(trim(lis_bene_cst_shr), '') is not null 
             then try_cast(lis_bene_cst_shr as float) 
             else null 
        end as lis_bene_cst_shr,
        
        case when nullif(trim(nonlis_bene_cst_shr), '') is not null 
             then try_cast(nonlis_bene_cst_shr as float) 
             else null 
        end as nonlis_bene_cst_shr,
        
        -- Senior population (65+) metrics
        case when nullif(trim(ge65_tot_clms), '') is not null 
             then try_cast(ge65_tot_clms as integer) 
             else null 
        end as ge65_tot_clms,
        
        case when nullif(trim(ge65_tot_benes), '') is not null 
             then try_cast(ge65_tot_benes as integer) 
             else null 
        end as ge65_tot_benes,
        
        case when nullif(trim(ge65_tot_30day_fills), '') is not null 
             then try_cast(ge65_tot_30day_fills as float) 
             else null 
        end as ge65_tot_30day_fills,
        
        case when nullif(trim(ge65_tot_drug_cst), '') is not null 
             then try_cast(ge65_tot_drug_cst as float) 
             else null 
        end as ge65_tot_drug_cst,
        
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
        
        -- Suppression flags
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
        
        -- Geographic information
        'NATIONAL' as prescriber_geo_level,
        'NATIONAL' as prescriber_geo_description
        
    from {{ source('raw', 'medicare_data') }}
)

select * from raw_medicare_data