{{ config(materialized="table") }}

/*


    Create a new model fct_fhv_monthly_zone_traveltime_p90.sql
    For each record in dim_fhv_trips.sql, compute the timestamp_diff in seconds between dropoff_datetime and pickup_datetime - we'll call it trip_duration for this exercise
    Compute the continous p90 of trip_duration partitioning by year, month, pickup_location_id, and dropoff_location_id

    For the Trips that respectively started from Newark Airport, SoHo, and Yorkville East, in November 2019, what are dropoff_zones with the 2nd longest p90 trip_duration ?

*/
with
    base_data as (
        select *, datetime_diff(dropoff_datetime, pickup_datetime, second) as time_diff
        from {{ ref("dim_fhv_trips") }}
        where pulocationid is not null and dolocationid is not null
    ),
    agg_data as (
        select
            year,
            month,
            pulocationid,
            dolocationid,
            pickup_zone,
            dropoff_zone,
            round(
                percentile_cont(time_diff, 0.90) over (
                    partition by year, month, pulocationid, dolocationid
                ),
                4
            ) as p90
        from base_data
    ),
    -- Remove duplicates since every trip in a group gets the same p90
    distinct_p90 as (
        select distinct year, month, pickup_zone, dropoff_zone, p90 from agg_data
    ),
    ranked_data as (
        select *, rank() over (partition by pickup_zone order by p90 desc) as rn
        from distinct_p90
        where
            year = 2019
            and month = 11
            and lower(pickup_zone) in ('newark airport', 'soho', 'yorkville east')
    )
select pickup_zone, dropoff_zone, p90
from ranked_data
where rn = 2
order by pickup_zone
