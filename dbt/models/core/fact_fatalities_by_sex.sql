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
    fatality_year, fatality_sex, count(*) as fatality_count 
from fact_fatalites
where fatality_year is not null
group by 
    fatality_year, fatality_sex
order by fatality_year