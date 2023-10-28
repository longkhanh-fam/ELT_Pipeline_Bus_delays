{{ config(
    materialized = "table",

) }}

with humidity as (
    select
        RecordDateTime,
        Humidity
    from {{ ref('humidity') }}
    where City = 'new_york'
),

temperature as (
    select
        RecordDateTime,
        Temperature
    from {{ ref('temperature') }}
    where City = 'new_york'
),

weather_description as (
    select
        RecordDateTime,
        Weather
    from {{ ref('weather_description') }}
    where City = 'new_york'
)

select
    -- time
    temperature.RecordDateTime as RecordDateTime,
    -- weather variables
    humidity.Humidity as Humidity,
    temperature.Temperature as Temperature,
    Weather
from temperature
inner join humidity
    on temperature.RecordDateTime = humidity.RecordDateTime
inner join weather_description
    on humidity.RecordDateTime = weather_description.RecordDateTime