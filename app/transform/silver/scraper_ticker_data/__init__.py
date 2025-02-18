from pyspark.sql.functions import get_json_object, col
from app.constant.lakehouse import SCRAPER_TICKERS_DATA
from app.transform.service.Spark import Spark as SparkService


def main():
    spark_service = SparkService()
    
    def df_transform_func(df):
        df = df.withColumn("price", get_json_object(col("data"), "$.price"))
        df = df.withColumn("dy", get_json_object(col("data"), "$.dy"))
        df = df.withColumn("pvp", get_json_object(col("data"), "$.pvp"))
        df = df.withColumn("roe", get_json_object(col("data"), "$.roe"))
        df = df.withColumn("segment", get_json_object(col("data"), "$.segment"))
        df = df.drop("data")

        return df

    spark_service.bronze_transform(table = SCRAPER_TICKERS_DATA, df_transform_func = df_transform_func)

if __name__ == "__main__":
    main()