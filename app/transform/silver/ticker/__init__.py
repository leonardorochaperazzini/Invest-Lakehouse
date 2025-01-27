from app.constant.lakehouse import DATABASE_LAKEHOUSE_SILVER, GLUE_CATALOG, S3_BRONZE_PATH, SILVER, TICKERS
from app.factory.SparkFactory import SparkFactory

def main():
    spark_factory = SparkFactory(iceberg=True, layer=SILVER)

    spark_session = spark_factory.build_spark()

    df_s3 = spark_session.read.parquet(f"{S3_BRONZE_PATH}/{TICKERS}")

    df_s3.createOrReplaceTempView("temp_view")

    iceberg_table_name = f"{GLUE_CATALOG}.{DATABASE_LAKEHOUSE_SILVER}.{TICKERS}"

    table_exists = spark_session.catalog.tableExists(iceberg_table_name)

    if table_exists:
        truncate_table_sql = f"""
            TRUNCATE TABLE {iceberg_table_name}
        """

        spark_session.sql(truncate_table_sql)

        insert_into_table_sql = f"""
            INSERT INTO {iceberg_table_name}
            SELECT * FROM temp_view;
        """
        spark_session.sql(insert_into_table_sql)

        print(f"Data inserted into existing Iceberg table: {iceberg_table_name}")
    else:
        create_table_sql = f"""
            CREATE TABLE {iceberg_table_name} USING iceberg AS
            SELECT * FROM temp_view;
        """
        spark_session.sql(create_table_sql)
        print(f"Data written to new Iceberg table: {iceberg_table_name}")

    df_iceberg = spark_session.table(iceberg_table_name)
    df_iceberg.show()

    spark_session.stop()

if __name__ == "__main__":
    main()