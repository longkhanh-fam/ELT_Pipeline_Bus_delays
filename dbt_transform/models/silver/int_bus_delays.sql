{{ config(
    materialized = "table",
    partition_by = { "field": "RecordDateTime", "data_type": "timestamp", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

select
    -- id
    RecordId,
    -- timestamp
    RecordDateTime,
    -- timestamp variants
    EXTRACT(DOW FROM cast(RecordDateTime as date)) as DayOfWeek,

    -- bus line id
    BusLineId,
    BusLineName,
    BusLineDirection,
    BusLineOrigin,
    BusLineDestination,
    -- bus stop Id
    BusStopId,
    -- bus delay
    EXTRACT(EPOCH FROM (ExpectedArrivalDateTime - ScheduledArrivalDateTime)) as DelaySeconds
from {{ref("bus_records")}}
where
    ExpectedArrivalDateTime is not null
    and ScheduledArrivalDateTime is not null
    -- we're only interested in the delay 'at the stop'
    and BusStatus = 'at stop'
