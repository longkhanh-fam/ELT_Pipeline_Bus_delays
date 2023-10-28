{{ config(
    materialized = "table"
) }}

select
    BusStopId,
    BusLineId,
    BusStopName,
    VehicleLocationLongitude, vehiclelocationlatitude

from {{ref("bus_records")}} bus_records
where BusStatus = 'at stop'
group by BusStopId, BusLineId, BusStopName, VehicleLocationLongitude, vehiclelocationlatitude