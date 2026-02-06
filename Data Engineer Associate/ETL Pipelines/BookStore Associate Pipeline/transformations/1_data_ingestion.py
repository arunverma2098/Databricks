from pyspark import pipelines as dp
from pyspark.sql import functions as F

dataset_path = spark.conf.get("dataset_path")

@dp.table()
def orders_raw():
    # schema =  "order_id STRING, order_timestamp LONG, customer_id STRING, quantity LONG"

    orders_raw_df = (spark.readStream
                          .format("cloudFiles")
                          .option("cloudFiles.format", "parquet")
                        #   .schema(schema)
                          .load(f"{dataset_path}/orders_raw")
                          )
    return orders_raw_df

@dp.table()
def customers():
    customers_df = (spark.read
                         .json(f"{dataset_path}/customers-json/")
                         )
    return customers_df