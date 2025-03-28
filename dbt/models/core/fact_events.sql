{{
    config(
        materialized='table'
    )
}}

with noaa_storms as (
    select * from {{ ref('stg_noaa_storms') }}
)

select 
    event_id,
    state,
    event_type,
    event_start_date,
    EXTRACT(MONTH FROM event_start_date) as event_start_month,
    EXTRACT(YEAR FROM event_start_date) as event_start_year,
    CONCAT(EXTRACT(YEAR FROM event_start_date), '-', EXTRACT(MONTH FROM event_end_date)) as event_start_year_month,
    event_end_date,
    injuries_direct,
    injuries_indirect,
    deaths_direct,
    deaths_indirect,
    damage_property,
    damage_crops,
    source,
    magnitude,
    magnitude_type
from noaa_storms

