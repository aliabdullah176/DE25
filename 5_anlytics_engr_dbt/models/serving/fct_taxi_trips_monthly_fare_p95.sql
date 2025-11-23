/*

Create a new model fct_taxi_trips_monthly_fare_p95.sql
Filter out invalid entries (fare_amount > 0, trip_distance > 0, and payment_type_description in ('Cash', 'Credit card'))
Compute the continous percentile of fare_amount partitioning by service_type, year and and month

Now, what are the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020?
*/
{{ config(materialized="table") }}

with
    base_data as (
        select
            *,
            extract(year from pickup_datetime) as year,
            extract(month from pickup_datetime) as month,
            extract(quarter from pickup_datetime) as quarter
        from {{ ref("facts_trip") }}
        where
            {{ dbt.safe_cast(("pickup_datetime"), api.Column.translate_type("date")) }}
            between
            {{ dbt.safe_cast('"2019-01-01"', api.Column.translate_type("date")) }}
            and {{ dbt.safe_cast('"2020-12-31"', api.Column.translate_type("date")) }}
            and fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ('Cash', 'Credit card')
    ),
    percentiles as (
        select
            service_type,
            year,
            month,
            approx_quantiles(fare_amount, 100)[offset(97)] as p97,
            approx_quantiles(fare_amount, 100)[offset(95)] as p95,
            approx_quantiles(fare_amount, 100)[offset(90)] as p90
        from base_data
        group by service_type, year, month
    )
select *
from percentiles
order by service_type, year, month
