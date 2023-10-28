from etl_pipeline.assets.bronze_layer import *
import pandasql as ps

DAILY_IN_JUNE = StaticPartitionsDefinition(
    [str(day) for day in range(1, 31)]
)
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver", "bus"],
    partitions_def=DAILY_IN_JUNE,
    ins={
        "bronze_bus_in_June": AssetIn(key_prefix=["bronze", "bus"],),
    },
    compute_kind="Minio"
)
def silver_bus_delays(bronze_bus_in_June) -> Output[pd.DataFrame]:
    df = pd.DataFrame(bronze_bus_in_June)

    # Execute the query on the DataFrame
    query = """
        SELECT RecordId, RecordDateTime, 
        strftime('%w', RecordDateTime) AS DayOfWeek,
        BusLineId, BusLineName, BusLineDirection, BusLineOrigin, BusLineDestination,
        BusStopId, (strftime('%s', ExpectedArrivalDateTime) - strftime('%s', ScheduledArrivalDateTime)) AS DelaySeconds
        FROM df
        WHERE ExpectedArrivalDateTime IS NOT NULL
        AND ScheduledArrivalDateTime IS NOT NULL
        AND BusStatus = 'at stop'
    """

    pysqldf = ps.sqldf(query, locals())

    return Output(
        pysqldf,
        metadata={
        "schema": "public",
        "table": "silver_bus_delays",
        "records counts": len(pysqldf),

    },
)

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    partitions_def=DAILY_IN_JUNE,
    key_prefix=["silver", "bus"],
    ins={
        "bronze_bus_in_June": AssetIn(key_prefix=["bronze", "bus"],),
    },
    compute_kind="Minio"
)
def silver_bus_stops(bronze_bus_in_June) -> Output[pd.DataFrame]:
    df = pd.DataFrame(bronze_bus_in_June)

    # Execute the query on the DataFrame
    query = """
        SELECT
            BusStopId,
            BusLineId,
            BusStopName,
            VehicleLocationLongitude,
            VehicleLocationLatitude
        FROM df
        WHERE BusStatus = 'at stop'
        GROUP BY BusStopId, BusLineId, BusStopName
        """

    pysqldf = ps.sqldf(query, locals())

    return Output(
        pysqldf,
        metadata={
        "schema": "public",
        "table": "silver_bus_stops",
        "records counts": len(pysqldf),

    },
)




@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    metadata={"full_load": True},
    partitions_def=DAILY_IN_JUNE,
    key_prefix=["silver", "weather"],
    ins={
        "bronze_temperature": AssetIn(key_prefix=["bronze", "weather"],     metadata={"full_load": True},),
        "bronze_weather_description": AssetIn(key_prefix=["bronze", "weather"],metadata={"full_load": True},),
        "bronze_humidity": AssetIn(key_prefix=["bronze", "weather"],metadata={"full_load": True},),

    },
    compute_kind="Minio"
)
def silver_weather_nyc(bronze_temperature, bronze_weather_description, bronze_humidity) -> Output[pd.DataFrame]:
    merged_df = pd.merge(bronze_temperature, bronze_humidity, on='RecordDateTime', how='inner')

    # Merge the merged DataFrame with the weather DataFrame on RecordDateTime
    final_df = pd.merge(merged_df, bronze_weather_description, on='RecordDateTime', how='inner')

    # Rename columns for clarity
    final_df.rename(columns={'Humidity': 'Humidity', 'Temperature': 'Temperature', 'Weather': 'Weather'}, inplace=True)

    # Select the desired columns
    result_df = final_df[['RecordDateTime', 'Humidity', 'Temperature', 'Weather']]

    return Output(
        result_df,
        metadata={
        "schema": "public",
        "table": "silver_weather_nyc",
        "records counts": len(result_df),

    },
)



