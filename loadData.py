import pandas as pd
from sqlalchemy import create_engine

import pymysql

db_user = "my_db_user"
db_password = "S3cret"
db_host = "de_mysql"
db_port = "3307"
db = "my_db"
engine = create_engine(
    f"mysql+pymysql://{db_user}:{db_password}@localhost:{db_port}/{db}")

df = pd.read_csv("D:/Khanh_FDE04/Project/trash/mta_1706/mta_1706_transformed.csv",on_bad_lines ='skip')
df.to_sql("bus_in_june", engine)
# #
# #busdelay
# query = """
#     SELECT
#     *
#     FROM silver_bus_delays
#
# """
#
# bus_delays = pd.read_sql(query, engine)
# bus_delays = pd.DataFrame(bus_delays)
# #bustop
#
# query = """
#     SELECT
#     *
#     FROM silver_bus_stops
#
# """
#
# bus_stops = pd.read_sql(query, engine)
# bus_stops = pd.DataFrame(bus_stops)
# #weather
# query = """
#     SELECT
#     *
#     FROM weather_nyc
#
# """
#
#
# weather_nyc = pd.read_sql(query, engine)
# weather_nyc = pd.DataFrame(weather_nyc)
#
#
# #----------------------------------------------
# #
# # SELECT
# #     -- id
# #     RecordId,
# #     -- timestamp
# #     bus_delays.RecordDateTime AS RecordDateTime,
# #     -- timestamp variants
# #     strftime('%Y-%m-%d %H:00:00', bus_delays.RecordDateTime) AS TruncatedRecordDateTime,
# #     -- bus line info
# #     bus_delays.BusLineId AS BusLineId,
# #     BusLineName,
# #     BusLineDirection,
# #     BusLineOrigin,
# #     BusLineDestination,
# #     -- bus stop info
# #     bus_delays.BusStopId AS BusStopId,
# #     BusStopName,
# #     -- bus delay
# #     DelaySeconds,
# #     -- weather info
# #     Weather,
# #     Humidity,
# #     Temperature
# # FROM bus_delays
# # INNER JOIN weather_nyc
# #     ON weather_nyc.RecordDateTime = strftime('%Y-%m-%d %H:00:00', bus_delays.RecordDateTime)
# # INNER JOIN bus_stops
# #     ON bus_stops.BusStopId = bus_delays.BusStopId
#
#
# query = """
#
#
#
# SELECT
#     -- id
#     RecordId,
#     -- timestamp
#     bus_delays.RecordDateTime AS RecordDateTime,
#     -- timestamp variants
#     --strftime('%Y-%m-%d %H:00:00', bus_delays.RecordDateTime) AS TruncatedRecordDateTime,
#     -- bus line info
#     bus_delays.BusLineId AS BusLineId,
#     BusLineName,
#     BusLineDirection,
#     BusLineOrigin,
#     BusLineDestination,
#     -- bus stop info
#     bus_delays.BusStopId AS BusStopId,
#     BusStopName,
#     VehicleLocationLongitude,
#     VehicleLocationLatitude,
#     -- bus delay
#     DelaySeconds,
#     -- weather info
#     Weather,
#     Humidity,
#     Temperature
# FROM bus_delays
# INNER JOIN weather_nyc
#     ON weather_nyc.RecordDateTime = strftime('%Y-%m-%d %H:00:00', bus_delays.RecordDateTime)
# INNER JOIN bus_stops
#     ON bus_stops.BusStopId = bus_delays.BusStopId
#
#
#     """
#
# pysqldf = ps.sqldf(query, locals())
#
# print(type(pysqldf))
#
# pysqldf.to_csv("D:/Khanh_FDE04/Project/trash/mta_1706/test.csv", index = False)
# #
# #
# # # from datetime import datetime
# # #
# # # # Sample timestamp
# # # timestamp_str = "2023-09-30 14:35:00"
# # # timestamp_str = bus_delays["RecordDateTime"][1005]
# # # timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
# # #
# # # # Truncate the timestamp to the nearest hour
# # # truncated_timestamp = timestamp.replace(minute=0, second=0)
# # #
# # # # Convert the truncated timestamp to a string with a specific format
# # # formatted_str = truncated_timestamp.strftime("%Y-%m-%d %H:%M:%S")
# # #
# # # # Print the formatted string
# # # # print(test)
# # # print((formatted_str))
# # # print(weather_nyc[weather_nyc["RecordDateTime"] == ])
#print("test")
