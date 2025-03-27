{{
    config(
        materialized='table'
    )
}}

with noaa_fatalities as (
    select * from {{ ref('stg_noaa_fatalities') }}
)

select 
    fatality_id,
    event_id,
    CASE WHEN fatality_type = 'I' THEN 'Indirect' 
         WHEN fatality_type = 'D' THEN 'Direct'
         ELSE 'Unknown' END as fatality_type,
    fatality_date,
    EXTRACT(MONTH FROM fatality_date) as fatality_month,
    EXTRACT(YEAR FROM fatality_date) as fatality_year,
    CASE WHEN fatality_sex = 'M' THEN 'Male' 
         WHEN fatality_sex = 'F' THEN 'Female'
         ELSE 'Unknown' END as fatality_sex,
    fatality_location
from noaa_fatalities
