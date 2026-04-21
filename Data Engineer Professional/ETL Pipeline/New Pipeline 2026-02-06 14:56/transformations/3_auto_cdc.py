from pyspark import pipelines as dp 
from pyspark.sql import functions as F

@dp.temporary_view
def customer_bronze_cdc():
    customer_schema = "customer_id String,email string,first_name string, last_name string, gender string, street string, city string, country_code string, row_status string, row_time timestamp"

    country_lookup_df = spark.read.table("country_lookup")

    return(spark.readStream
           .table("bronze")
           .filter("topic = 'customers'")
           .select(F.from_json(F.unbase64(F.col("value")).cast("string"),customer_schema).alias("v"))
           .select("v.*")
           .join(F.broadcast(country_lookup_df), F.col("country_code") == F.col("code"), "inner"))
    
dp.create_streaming_table("customers_silver")

dp.create_auto_cdc_flow(
    target = "customers_silver",
    source = "customer_bronze_cdc",
    keys = ["customer_id"],
    sequence_by = F.col("row_time"),
    except_column_list = ["row_status", "row_time"]
    # apply_as_deletes= F.expr("row_status = 'delete'")
)
    
@dp.materialized_view
def countries_stats():
    orders_df = spark.read.table("orders_silver")
    customers_df = spark.read.table("customers_silver")

    return(orders_df.join(customers_df, orders_df["customer_id"]==customers_df["customer_id"], "inner")
                    .withColumn("order_date", F.date_trunc("DAY", F.col("order_timestamp")))
                    .groupBy("country", "order_date")
                    .agg(
                        F.count("order_id").alias("orders_count"),
                        F.sum("quantity").alias("books_count")
                    )
           )
