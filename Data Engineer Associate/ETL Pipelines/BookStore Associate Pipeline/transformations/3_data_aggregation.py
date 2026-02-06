from pyspark import pipelines as dp 
from pyspark.sql import functions as F

@dp.table()
def cn_daily_customer_books():
    cn_daily_customer_books_df = (
        spark.read.table("orders_cleaned")
                    .filter(F.col("country")=="China")
                    .withColumn("order_date", F.date_trunc("dd",F.col("order_timestamp")))
                    .groupBy(F.col("customer_id"), F.col("f_name"), F.col("l_name"), F.col("order_date"))
                    .agg(F.sum(F.col("quantity")).alias("books_count")))
    return cn_daily_customer_books_df

