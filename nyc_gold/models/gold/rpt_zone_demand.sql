{{ config(materialized='table', schema='gold') }}
select
    z.zone_name,
    z.borough,
    f.pickup_day_of_week,
    f.pickup_hour,
    f.trip_count,
    f.avg_fare
from {{ ref('fact_trips') }} as f
inner join {{ ref('dim_zone') }} as z on f.zone_id = z.zone_id
