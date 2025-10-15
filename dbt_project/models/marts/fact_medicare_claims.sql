{{
    config(
        materialized='table',
        cluster_by=['service_year', 'drug_category']
    )
}}

with medicare_base as (
    select
        cast(service_year as integer) as service_year,
        cast(drug_id as string) as drug_id,
        upper(cast(brnd_name as string)) as brand_name,
        upper(cast(gnrc_name as string)) as generic_name,
        cast(tot_clms as integer) as total_claims,
        cast(tot_benes as integer) as total_beneficiaries,
        cast(tot_prscrbrs as integer) as total_prescribers,
        cast(tot_30day_fills as numeric) as total_30day_fills,
        cast(tot_drug_cst as numeric) as total_drug_cost,
        cast(lis_bene_cst_shr as numeric) as low_income_cost_share,
        cast(nonlis_bene_cst_shr as numeric) as non_low_income_cost_share,
        cast(is_antibiotic_drug as boolean) as is_antibiotic_drug,
        cast(is_opioid_drug as boolean) as is_opioid_drug,
        cast(is_long_acting_opioid as boolean) as is_long_acting_opioid,
        cast(is_antipsychotic_drug as boolean) as is_antipsychotic_drug,
        -- Handle NULL values for 65+ data with COALESCE
        cast(coalesce(ge65_tot_clms, 0) as integer) as ge65_total_claims,
        cast(coalesce(ge65_tot_benes, 0) as integer) as ge65_total_beneficiaries,
        cast(coalesce(ge65_tot_30day_fills, 0) as numeric) as ge65_total_30day_fills,
        cast(coalesce(ge65_tot_drug_cst, 0) as numeric) as ge65_total_drug_cost
    from {{ ref('stg_medicare_data') }}
    where drug_id is not null 
      and service_year is not null 
      and brnd_name is not null 
      and gnrc_name is not null
),

enriched_data as (
    select
        service_year,
        drug_id,
        brand_name,
        generic_name,
        total_claims,
        total_beneficiaries,
        total_prescribers,
        total_30day_fills,
        total_drug_cost,
        low_income_cost_share,
        non_low_income_cost_share,
        is_antibiotic_drug,
        is_opioid_drug,
        is_long_acting_opioid,
        is_antipsychotic_drug,
        ge65_total_claims,
        ge65_total_beneficiaries,
        ge65_total_30day_fills,
        ge65_total_drug_cost,
        -- Derived metrics with NULL protection
        round(total_drug_cost / nullif(total_claims, 0), 2) as cost_per_claim,
        round(total_claims / nullif(total_prescribers, 0), 2) as claims_per_prescriber,
        round(ge65_total_claims / nullif(total_claims, 0), 4) as ge65_claim_ratio,
        -- Enhanced drug category classification
        case
            when is_opioid_drug and is_long_acting_opioid then 'LONG_ACTING_OPIOID'
            when is_opioid_drug then 'SHORT_ACTING_OPIOID'
            when is_antibiotic_drug then 'ANTIBIOTIC' 
            when is_antipsychotic_drug then 'ANTIPSYCHOTIC'
            else 'OTHER'
        end as drug_category
    from medicare_base
)


select
    -- UUID Primary Key (drug_id + service_year)
    md5(
        cast(drug_id as string) || '-' || 
        cast(service_year as string)
    ) as claim_id,
    service_year,
    drug_id,
    brand_name,
    generic_name,
    total_claims,
    total_beneficiaries,
    total_prescribers,
    total_30day_fills,
    total_drug_cost,
    low_income_cost_share,
    non_low_income_cost_share,
    is_antibiotic_drug,
    is_opioid_drug,
    is_long_acting_opioid,
    is_antipsychotic_drug,
    ge65_total_claims,
    ge65_total_beneficiaries,
    ge65_total_30day_fills,
    ge65_total_drug_cost,
    cost_per_claim,
    claims_per_prescriber,
    ge65_claim_ratio,
    drug_category
from enriched_data