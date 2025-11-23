{{ config(materialized="view") }}

with
    fhv_data as (select * from {{ ref("stg_ny_fhv_taxi") }}),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    fhv_data.*,
    coalesce(pickup_zone.borough, "MISSING") as pickup_borough,
    coalesce(pickup_zone.zone, "MISSING") as pickup_zone,
    coalesce(dropoff_zone.borough, "MISSING") as dropoff_borough,
    coalesce(dropoff_zone.zone, "MISSING") as dropoff_zone,
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month,
    extract(quarter from pickup_datetime) as quarter
from fhv_data
inner join
    dim_zones as pickup_zone on fhv_data.pulocationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone on fhv_data.dolocationid = dropoff_zone.locationid
