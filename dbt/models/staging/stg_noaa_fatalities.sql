with

    source as (

        select *, row_number() over (partition by fatality_id) as rn
        from {{ source("staging", "noaa_fatalities_partitioned") }}

    ),

    renamed as (

        select
            fatality_id,
            event_id,
            fatality_type,
            fatality_date,
            fatality_age,
            fatality_sex,
            fatality_location

        from source
        where rn = 1

    )

select *
from renamed
