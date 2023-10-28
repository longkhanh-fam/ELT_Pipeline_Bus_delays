from dagster import Definitions, load_assets_from_modules, file_relative_path
from dagster_dbt import dbt_cli_resource
from . import assets
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources.spark_io_manager import SparkIOManager
all_assets = load_assets_from_modules([assets])
# DBT_PROJECT_PATH = file_relative_path(__file__, ".D:/Khanh_FDE04/project/dbt_transform")
# DBT_PROFILES = file_relative_path(__file__, "D:/Khanh_FDE04/Project/dbt_transform/config")

MINIO_CONFIG = {
    "endpoint_url": "minio:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
MYSQL_CONFIG = {
    "host": "de_mysql",
    "port": 3306,
    "database": "my_db",
    "user": "my_db_user",
    "password": "S3cret",
}

PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}


SPARK_CONFIG = {
    "spark_master": "spark://spark-master:7077",
    "spark_version": "3.4.1",
    "hadoop_version": "3",
    "endpoint_url": "minio:9000",
    "minio_access_key": "minio",
    "minio_secret_key": "minio123",
}

# DBT_PROJECT_PATH = file_relative_path(__file__, "/bus_delays")
# DBT_PROFILES = file_relative_path(__file__, "/bus_delays/config")

resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG),
    # "dbt": dbt_cli_resource.configured(
    #     {
    #         "project_dir": DBT_PROJECT_PATH,
    #         "profiles_dir": DBT_PROFILES,
    #     }
    # ),
}
defs = Definitions(
    assets=load_assets_from_modules([assets]),

    resources=resources,

)
