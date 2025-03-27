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
    fatality_month, count(1) as fatality_count
from fact_fatalites
where fatality_month is not null
group by 
    fatality_month
order by fatality_month