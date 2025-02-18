from pyspark.sql import functions as F
from app.constant.lakehouse import USERS
from app.transform.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    def df_transform_func(df):
        df = df.withColumn("email", F.concat(F.substring(df.email, 1, 5), F.lit("****")))
        df = df.drop("hashed_password")
        return df

    spark_service.bronze_transform(table = USERS, df_transform_func = df_transform_func)

if __name__ == "__main__":
    main()