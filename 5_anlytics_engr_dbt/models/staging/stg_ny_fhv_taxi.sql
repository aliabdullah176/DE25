with

    source as (select * from {{ source("staging", "ny_fhv_taxi") }}),

    renamed as (

        select
            dispatching_base_num,
            pickup_datetime,
            dropoff_datetime,
            pulocationid,
            dolocationid,
            sr_flag,
            affiliated_base_number

        from source

        where dispatching_base_num is not null

    )

select *
from renamed
