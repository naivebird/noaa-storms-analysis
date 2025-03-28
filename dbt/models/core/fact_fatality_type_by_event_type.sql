{{
    config(
        materialized='table'
    )
}}

with fact_events as (
    select *
    from {{ ref("fact_events") }}
),

fact_fatalites as (
    select *
    from {{ ref("fact_fatalities") }}
)

select
    event_start_year, event_type, fatality_type, count(fact_fatalites.fatality_id) as fatality_count
from fact_events
join fact_fatalites
on fact_events.event_id = fact_fatalites.event_id
group by
    event_start_year,
    event_type,
    fatality_type
order by event_type