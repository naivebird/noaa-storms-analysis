{{
    config(
        materialized='table'
    )
}}

with fact_events as (
    select *
    from {{ ref("fact_events") }}
)

select
    event_type, sum(damage_crops + damage_property) as cost
from fact_events
group by 
    event_type