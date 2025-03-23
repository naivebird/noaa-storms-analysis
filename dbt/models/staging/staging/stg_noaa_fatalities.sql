with 

source as (

    select * from {{ source('staging', 'noaa_fatalities_partitioned') }}

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

)

select * from renamed
