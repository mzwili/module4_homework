{{ config(materialized='view') }}

with fhv_data as (
    select * from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null
)

select
    -- Identifiers
    dispatching_base_num,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    -- Timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- Trip info
    sr_flag,
    affiliated_base_number
from fhv_data
-- Filter for 2019 data as per the Zoomcamp requirements
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2020-01-01'
