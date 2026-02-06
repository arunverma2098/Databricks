from pyspark import pipelines as dp 
from pyspark.sql import functions as F

@dp.table()
@dp.expect_or_drop("valid_order_number", "order_id is not null")

def orders_cleaned():
    orders_refined = (spark.readStream.table("orders_raw")
                            .select(F.col("order_id"),
                                         F.col("customer_id"), 
                                         F.col("quantity"),
                                         F.col("order_timestamp"))
                            .withColumn("order_timestamp", F.from_unixtime(F.col("order_timestamp"),'yyyy-MM-dd HH:mm:ss').cast("timestamp")))
                                         
    customers_refined = (spark.read.table("customers")
                              .selectExpr("customer_id",
                              "profile:first_name as f_name",
                              "profile:last_name as l_name",
                              "profile:address:country as country"))
    
    orders_cleaned_df = (orders_refined.join(customers_refined, "customer_id","inner")
                                   .select(orders_refined.order_id,
                                           orders_refined.customer_id,
                                           orders_refined.quantity,
                                           orders_refined.order_timestamp,
                                           customers_refined.f_name,
                                           customers_refined.l_name,
                                           customers_refined.country))
    
    return orders_cleaned_df
                                   
    
