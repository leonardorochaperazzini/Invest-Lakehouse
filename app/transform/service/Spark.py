import os
import pytz
from datetime import datetime
from app.constant.lakehouse import DATABASE_LAKEHOUSE_SILVER, S3_BRONZE_PATH, SILVER
from app.factory.SparkSessionFactory import SparkSessionFactory


class Spark():
    def __init__(self, logger = os.environ.get("PRINT_LOG"), force_recreate_on_fail = True):
        spark_session_factory = SparkSessionFactory(iceberg=True, layer=SILVER)

        self.logger = logger
        self.force_recreate_on_fail = force_recreate_on_fail
        self.spark_session = spark_session_factory.build_spark()
        self.jdbc_url, self.connection_properties = spark_session_factory.build_postgres_connection_properties()

    def __drop_table(self, table_name):
        drop_table_sql = f"DROP TABLE {table_name}"
        self.spark_session.sql(drop_table_sql)

    def __add_new_columns_to_iceberg_table(self, target_table, new_columns):
        for column_name, column_type in new_columns.items():
            alter_table_sql = f"ALTER TABLE {target_table} ADD COLUMN {column_name} {column_type.simpleString()}"
            self.spark_session.sql(alter_table_sql)

    def __check_table_exists(self):
        table_exists = self.spark_session.catalog.tableExists(self.iceberg_table_name)

        return table_exists
    
    def __insert_into_existing_table(self, df):
        target_table_schema = self.spark_session.table(self.iceberg_table_name).schema
        target_columns = [field.name for field in target_table_schema.fields]

        new_columns = {col: df.schema[col].dataType for col in df.columns if col not in target_columns}

        if new_columns:
            self.__add_new_columns_to_iceberg_table(self.iceberg_table_name, new_columns)

            target_table_schema = self.spark_session.table(self.iceberg_table_name).schema
            target_columns = [field.name for field in target_table_schema.fields]
        
        df = df.select(*[col for col in target_columns if col in df.columns])

        self.spark_session.sql(f"TRUNCATE TABLE {self.iceberg_table_name}")
    
        df.write.mode("append").insertInto(self.iceberg_table_name)
        
        if self.logger:
            df.show()
            print(f"Data inserted into existing Iceberg table: {self.iceberg_table_name}")

    def __insert_into_new_table(self, df):
        create_table_sql = f"CREATE TABLE {self.iceberg_table_name} USING iceberg AS SELECT * FROM temp_view"
        self.spark_session.sql(create_table_sql)
        if self.logger:
            df.show()
            print(f"Data written to new Iceberg table: {self.iceberg_table_name}")

    def bronze_transform(self, table, s3_path = S3_BRONZE_PATH, df_transform_func = None):
        utc_now = datetime.now(pytz.utc)
        current_date = utc_now.strftime("%Y-%m-%d")

        path = f"{s3_path}/{table}/inserted_date={current_date}"

        df = self.spark_session.read.parquet(path)

        if df_transform_func:
            df = df_transform_func(df)

        df.createOrReplaceTempView("temp_view")

        self.iceberg_table_name = f"{DATABASE_LAKEHOUSE_SILVER}.{table}"

        if self.__check_table_exists():
            try:
                self.__insert_into_existing_table(df)
            except Exception as e:
                if self.force_recreate_on_fail:
                    df.show()
                    if 'the reason is not enough data columns' in str(e) or 'Consider to choose another name or rename the existing column' in str(e):
                        print(f"Failed to insert into existing table: {self.iceberg_table_name}, dropping table and creating new table")
                        self.__drop_table(self.iceberg_table_name)
                        self.__insert_into_new_table(df)
                else:
                    raise e
        else:
            self.__insert_into_new_table(df)

        self.spark_session.stop()
