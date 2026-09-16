{{ config(materialized='table', schema='gold') }}

select
    PULocationID              as zone_id,
    pickup_day_of_week,
    pickup_hour,
    count(*)                  as trip_count,
    avg(fare_amount)          as avg_fare,
    avg(trip_distance)        as avg_trip_distance,
    avg(trip_duration)        as avg_trip_duration
from {{ source('silver', 'silver_yellowtaxi') }}
group by PULocationID, pickup_day_of_week, pickup_hour