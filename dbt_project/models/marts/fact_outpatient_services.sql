{{
    config(
        materialized='table',
        cluster_by=['service_year', 'provider_id']
    )
}}

with outpatient_base as (
    select
        cast(service_year as integer) as service_year,
        split_part(apc, ' - ', 1) as apc_code,
        split_part(apc, ' - ', 2) as apc_description,
        cast(provider_id as string) as provider_id,
        cast(outpatient_services_count as integer) as service_volume,
        cast(average_total_payments as numeric) as average_payment_amount,
        cast(average_submitted_charges as numeric) as average_submitted_charge
    from {{ ref('stg_outpatient_charges') }}
    where provider_id is not null 
      and service_year is not null 
      and apc is not null
),

final_fact_outpatient as (
    select
        -- Surrogate Key
        md5(
            cast(ob.provider_id as string) || '-' || 
            cast(ob.service_year as string) || '-' || 
            cast(ob.apc_code as string)
        ) as outpatient__id,
        
        -- Foreign Keys
        ob.provider_id,
        ob.service_year,
        upper(trim(ob.apc_code)) as apc_code,
        
        -- Measures
        ob.service_volume,
        ob.average_payment_amount,
        ob.average_submitted_charge,
        
        -- Derived metrics
        round(ob.average_payment_amount / nullif(ob.average_submitted_charge, 0), 4) as payment_to_charge_ratio
    from outpatient_base ob
    where ob.apc_code != '' 
)

select * from final_fact_outpatient