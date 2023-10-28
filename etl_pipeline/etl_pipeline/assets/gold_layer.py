from dagster import LastPartitionMapping

from etl_pipeline.assets.silver_layer import *



DAILY_IN_JUNE = StaticPartitionsDefinition(
    [str(day) for day in range(1, 31)]
)



# @multi_asset(
#     ins={
#         "silver_bus_delays": AssetIn(key_prefix=["silver", "bus"],),
#         "silver_bus_stops": AssetIn(key_prefix=["silver", "bus"],),
#         "silver_weather_nyc": AssetIn(key_prefix=["silver", "weather"],),
#     },
#
#     outs={
#         "gold_bus_delays": AssetOut(
#         io_manager_key="psql_io_manager",
#         key_prefix=["warehouse", "gold"],
#         metadata={
#         },
#
#         )
#     },
#     compute_kind="PostgreSQL")
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "bus"],
    partitions_def=DAILY_IN_JUNE,
    required_resource_keys={"minio_io_manager"},
    ins={
                "silver_bus_delays": AssetIn(
                    key_prefix=["silver", "bus"],

                ),
                "silver_bus_stops": AssetIn(
                    key_prefix=["silver", "bus"],

                ),
                "silver_weather_nyc": AssetIn(
                    key_prefix=["silver", "weather"],
metadata={"full_load": True}
                ),
    },
    compute_kind="Minio"
)
def gold_bus_delays(silver_bus_delays, silver_bus_stops, silver_weather_nyc) -> Output[pd.DataFrame]:
    bus_delays = pd.DataFrame(silver_bus_delays)
    bus_stops = pd.DataFrame(silver_bus_stops)
    weather_nyc = pd.DataFrame(silver_weather_nyc)

    query = """



    SELECT
        -- id
        RecordId,
        -- timestamp
        bus_delays.RecordDateTime AS RecordDateTime,
        -- timestamp variants
        bus_delays.DayOfWeek,
        --strftime('%Y-%m-%d %H:00:00', bus_delays.RecordDateTime) AS TruncatedRecordDateTime,
        -- bus line info
        bus_delays.BusLineId AS BusLineId,
        BusLineName,
        BusLineDirection,
        BusLineOrigin,
        BusLineDestination,
        -- bus stop info
        bus_delays.BusStopId AS BusStopId,
        BusStopName,
        VehicleLocationLongitude,
        VehicleLocationLatitude,
        -- bus delay
        DelaySeconds,
        -- weather info
        Weather,
        Humidity,
        Temperature
    FROM bus_delays
    INNER JOIN weather_nyc
        ON weather_nyc.RecordDateTime = strftime('%Y-%m-%d %H:00:00', bus_delays.RecordDateTime)
    INNER JOIN bus_stops
        ON bus_stops.BusStopId = bus_delays.BusStopId


        """

    pysqldf = ps.sqldf(query, locals())
    return Output(
        pysqldf,
        metadata={
        "schema": "gold",
        "table": "gold_bus_delays",
        "records counts": len(pysqldf),
    },
)

@asset(
    io_manager_key="psql_io_manager",
    required_resource_keys={"minio_io_manager"},
    partitions_def=DAILY_IN_JUNE,
    key_prefix=["warehouse", "public"],
    ins={
        "gold_bus_delays": AssetIn(key_prefix=["gold", "bus"],),
    },
    compute_kind="PostgreSQL"
)
def bus_delays(gold_bus_delays) -> Output[pd.DataFrame]:
    gold_bus_delays.columns = gold_bus_delays.columns.str.lower()
    return Output(
        gold_bus_delays,
        metadata={
        "schema": "public",
        "table": "gold_bus_delays",
        "records counts": len(gold_bus_delays),
    },
)


@asset(
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "public"],
    partitions_def=DAILY_IN_JUNE,
    required_resource_keys={"minio_io_manager"},
    ins={
                "silver_bus_stops": AssetIn(
                    key_prefix=["silver", "bus"],

                ),
    },
    compute_kind="PostgreSQL"
)
def bus_stops(silver_bus_stops) -> Output[pd.DataFrame]:
    silver_bus_stops.columns = silver_bus_stops.columns.str.lower()
    return Output(
        silver_bus_stops,
        metadata={
        "schema": "public",
        "table": "bus_stops",
        "records counts": len(silver_bus_stops),
    },
)
