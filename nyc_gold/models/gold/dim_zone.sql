{{ config(materialized='table', schema='gold' ) }}

select
    locationid as zone_id,
    zone as zone_name,
    borough as borough,
    service_zone as service_zone
from {{ ref('taxi_zone_lookup') }}
