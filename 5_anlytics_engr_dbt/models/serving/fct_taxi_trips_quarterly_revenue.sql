/*
    Create a new model fct_taxi_trips_quarterly_revenue.sql
    Compute the Quarterly Revenues for each year for based on total_amount
    Compute the Quarterly YoY (Year-over-Year) revenue growth

    e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
    e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4
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
            date(pickup_datetime) > "2018-12-31"
            and date(pickup_datetime) < "2021-01-01"
    ),
    quarterly_revenues as (
        select year, quarter, service_type, sum(total_amount) as revenue
        from base_data
        group by year, quarter, service_type
    ),
    lag_revenues as (
        select
            *,
            lag(revenue) over (
                partition by quarter, service_type order by year asc
            ) as revenue_last_q
        from quarterly_revenues
    ),
    yoy_q_growth as (
        select
            *,
            round(
                100 * ({{ dbt_utils.safe_divide("revenue", "revenue_last_q") }} - 1), 2
            ) as growth
        from lag_revenues
    )
select *
from yoy_q_growth
order by service_type, year, quarter
