{{ config(
    materialized = "table",

) }}

with weather_nyc as (
    select *
    from {{ ref('weather_nyc') }}
),

bus_stops as (
    select *
    from {{ ref('bus_stops') }}
)

select
    -- id
    RecordId,
    -- timestamp
    bus_delays.RecordDateTime as RecordDateTime,
    -- timestamp variants

    -- bus line info
    bus_delays.BusLineId as BusLineId,
    BusLineName,
    BusLineDirection,
    BusLineOrigin,
    BusLineDestination,
    -- bus stop info
    bus_delays.BusStopId as BusStopId,
    BusStopName,
    VehicleLocationLongitude, vehiclelocationlatitude,
    -- bus delay
    DelaySeconds,
    -- weather info
    Weather,
    Humidity,
    Temperature
from {{ ref('bus_delays') }} bus_delays
inner join weather_nyc
    on date_trunc('hour', bus_delays.RecordDateTime) = date_trunc('hour', weather_nyc.RecordDateTime)
inner join bus_stops
    on bus_stops.BusStopId = bus_delays.BusStopId