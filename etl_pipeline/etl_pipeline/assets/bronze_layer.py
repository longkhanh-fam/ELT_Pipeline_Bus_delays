import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, multi_asset,AssetOut, StaticPartitionsDefinition, SourceAsset, DailyPartitionsDefinition
from etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.resources.mysql_io_manager import MySQLIOManager
from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager
from etl_pipeline.resources.spark_io_manager import SparkIOManager



DAILY_IN_JUNE = StaticPartitionsDefinition(
    [str(year) for year in range(1, 31)]
)
@asset(

    io_manager_key="minio_io_manager",
    partitions_def=DAILY_IN_JUNE,
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "bus"],
    compute_kind="MySQL"
)

def bronze_bus_in_June(context) -> Output[pd.DataFrame]:
    query = """
    SELECT
        -- id
        md5(CONCAT(RecordedAtTime, PublishedLineName, DirectionRef, OriginName, DestinationName, VehicleRef)) AS RecordId,
        -- timestamps
        CAST(RecordedAtTime AS DATETIME) AS RecordDateTime,
        CAST(ExpectedArrivalTime AS DATETIME) AS ExpectedArrivalDateTime,
        CAST(ScheduledArrivalTime AS DATETIME) AS ScheduledArrivalDateTime,
        -- bus line id & info
        md5(CONCAT(PublishedLineName, DirectionRef, OriginName, DestinationName)) AS BusLineId,
        PublishedLineName AS BusLineName,
        DirectionRef AS BusLineDirection,
        OriginName AS BusLineOrigin,
        DestinationName AS BusLineDestination,
        -- geographical location of vehicle
        ST_GeomFromText(CONCAT('POINT(', VehicleLocationLongitude, ' ', VehicleLocationLatitude, ')')) AS VehicleLocation,
        VehicleLocationLongitude, 
        VehicleLocationLatitude,

        -- bus stop info
        md5(CONCAT(PublishedLineName, DirectionRef, OriginName, DestinationName, NextStopPointName)) AS BusStopId,
        NextStopPointName AS BusStopName,
        ArrivalProximityText AS BusStatus
    FROM bus_in_june1
    """

    try:
        partion_date_str = context.asset_partition_key_for_output()
        partition_by = "SUBSTRING(RecordedAtTime, 9, 2)"
        query += f" WHERE {partition_by} = {partion_date_str}"
        context.log.info(f"Partition by {partition_by} = {partion_date_str}")
    except Exception:
        context.log.info("No partition key found, full load data")

    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")


    return Output(
        value=df_data,
        metadata={
            "table": "bus",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],

        },
    )




@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    partitions_def=DAILY_IN_JUNE,
    key_prefix=["bronze", "weather"],
    compute_kind="MySQL",
    metadata={"full_load": True},
)

def bronze_temperature(context) -> Output[pd.DataFrame]:
    # define SQL statement
    sql_stm = '''
    WITH unpivoted AS (
        SELECT
            datetime,
            'New York' AS City,
            CAST(new_york AS DOUBLE) AS Temperature
        FROM temperature
    )
    SELECT
        -- identifiers
        CONCAT(datetime, City) AS RecordId,
        -- timestamps
        TIMESTAMP(datetime) AS RecordDateTime,
        -- city
        City,
        -- value (convert from kelvin to celsius)
        (Temperature - 273.15) AS Temperature
    FROM unpivoted;
    '''

    # Now you can use the "query" string in your Python code to execute the SQL query.

    # use context with resources mysql_io_manager (defined by required_resource_keys)


    # using extract_data() to retrieve data as Pandas Dataframe
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    # return Pandas Dataframe with metadata information
    return Output(
        pd_data,
        metadata={
        "table": "temperature",
        "row_count": pd_data.shape[0],
        "column_count": pd_data.shape[1],
         },
    )



@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "weather"],
    compute_kind="MySQL",
    metadata={"full_load": True},
    partitions_def=DAILY_IN_JUNE,
)

def bronze_weather_description(context) -> Output[pd.DataFrame]:
    # define SQL statement
    sql_stm = '''
 WITH unpivoted AS (
    SELECT
        datetime,
        'New York' AS City,
        CAST(new_york AS CHAR) AS Weather
    FROM weather_description
    )
    SELECT
        -- identifiers
        md5(CONCAT(datetime, City)) AS RecordId,
        -- timestamps
        TIMESTAMP(datetime) AS RecordDateTime,
        -- city
        City,
        -- value
        Weather
    FROM unpivoted;
    '''

    # Now you can use the "query" string in your Python code to execute the SQL query.

    # use context with resources mysql_io_manager (defined by required_resource_keys)


    # using extract_data() to retrieve data as Pandas Dataframe
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    # return Pandas Dataframe with metadata information
    return Output(
        pd_data,
        metadata={
        "table": "weather_description",
        "row_count": pd_data.shape[0],
        "column_count": pd_data.shape[1],
         },
    )



@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "weather"],
    compute_kind="MySQL",
    metadata={"full_load": True},
    partitions_def=DAILY_IN_JUNE,
)

def bronze_humidity(context) -> Output[pd.DataFrame]:
    # define SQL statement
    sql_stm = '''
    WITH unpivoted AS (
    SELECT
        datetime,
        'New York' AS City,
        CAST(new_york AS DOUBLE) AS Humidity
    FROM humidity
    )
    SELECT
        -- identifiers
        md5(CONCAT(datetime, City)) AS RecordId,
        -- timestamps
        TIMESTAMP(datetime) AS RecordDateTime,
        -- city
        City,
        -- value
        Humidity
    FROM unpivoted;

    '''

    # Now you can use the "query" string in your Python code to execute the SQL query.

    # use context with resources mysql_io_manager (defined by required_resource_keys)


    # using extract_data() to retrieve data as Pandas Dataframe
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    # return Pandas Dataframe with metadata information
    return Output(
        pd_data,
        metadata={
        "table": "humidity",
        "row_count": pd_data.shape[0],
        "column_count": pd_data.shape[1],
         },
    )

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "database": "my_db",
    "user": "my_db_user",
    "password": "S3cret",
}

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
defs = Definitions(
    assets=[bronze_temperature, bronze_humidity, bronze_weather_description, bronze_bus_in_June

            ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },

)