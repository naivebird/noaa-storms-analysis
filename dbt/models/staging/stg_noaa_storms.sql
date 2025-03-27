with

    source as (
        select *, row_number() over (partition by event_id) as rn
        from {{ source("staging", "noaa_storms_partitioned") }}

    ),

    renamed as (

        select
            event_id,
            state,
            event_type,
            event_start_date,
            event_end_date,
            injuries_direct,
            injuries_indirect,
            deaths_direct,
            deaths_indirect,
            damage_property,
            damage_crops,
            source.source,
            magnitude,
            magnitude_type

        from source
        where rn = 1

    )

select *
from renamed
