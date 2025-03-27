{{
    config(
        materialized='table'
    )
}}


with fact_fatalites as (
    select *
    from {{ ref("fact_fatalities") }}
)

select
    fatality_sex, count(*) as fatality_count 
from fact_fatalites
group by 
    fatality_sex