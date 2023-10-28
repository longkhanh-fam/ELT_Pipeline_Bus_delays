import pandas as pd
from dagster import IOManager, OutputContext, InputContext

from minio import Minio
from minio.error import S3Error
import os
import csv

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.client = Minio(
            "minio:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )

    def _get_path(self,  context: OutputContext):

        layer, schema, table = context.asset_key.path

        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])

        file_name = context.asset_key.path[-1]
        tmp_file_path = f"/tmp/{file_name}.csv"
        full_load = (context.metadata or {}).get("full_load", False)
        if context.has_partition_key:
            if full_load:
                return f"{key}.csv", tmp_file_path
            partition_str = str(table) + "_" + context.asset_partition_key
            return os.path.join(key, f"{partition_str}.csv"), tmp_file_path
        else:

            return f"{key}.csv", tmp_file_path



    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        bucket_name = self._config['bucket']



        key_name, tmp_file_path = self._get_path(context)

        obj.to_csv(tmp_file_path, index=False)
        #obj.to_parquet(tmp_file_path)

        found = self.client.bucket_exists(bucket_name)
        if not found:
           self.client.make_bucket(bucket_name)
        else:
            print(f"Bucket {bucket_name} already exists")


        # upload to MinIO <bucket_name>/<key_prefix>/<your_file_name>.csv
        try:
            self.client.fput_object(bucket_name, key_name, tmp_file_path)
        except S3Error as exc:
           print("error occurred.", exc)
        #
        pass

    def load_input(self, context: InputContext) -> pd.DataFrame:

        bucket_name = self._config['bucket']
        key_name, tmp_file_path = self._get_path(context)



        found = self.client.bucket_exists(bucket_name)
        if not found:
           self.client.make_bucket(bucket_name)
        else:
            print(f"Bucket {bucket_name} already exists")

        full_load = (context.metadata or {}).get("full_load", False)
        df_data = pd.DataFrame()
        # if full_load:
        #     prefix = os.path.splitext(key_name)[0] + "/"
        #     objects = self.client.list_objects(
        #         bucket_name, prefix=prefix,
        #         include_version=True
        #     )
        #     context.log.info(prefix)
        #     for obj in objects:
        #         self.client.fget_object(bucket_name, obj.object_name, tmp_file_path )
        #         context.log.info( obj.object_name)
        #         df1 = pd.read_csv(tmp_file_path)
        #         df_data = pd.concat([df_data, df1])
        # else:
        self.client.fget_object(bucket_name, key_name, tmp_file_path)
        df_data = pd.read_csv(tmp_file_path, on_bad_lines ='skip')




        return df_data