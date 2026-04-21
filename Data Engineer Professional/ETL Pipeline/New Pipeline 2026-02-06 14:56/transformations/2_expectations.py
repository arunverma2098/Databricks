from pyspark import pipelines as dp
from pyspark.sql import functions as F

def process_orders():
    orders_schema = "order_id STRING, order_timestamp timestamp, customer_id STRING, quantity BIGINT, total BIGINT,books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

    query = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'orders'")
                    .select(F.from_json(F.unbase64(F.col("value")).cast("string"), orders_schema).alias("v"))
                    .select("v.*")
                    .withColumn("load_timestamp", F.current_timestamp())
    )
    return query

@dp.table
@dp.expect_or_drop("vaild quantity", "quantity > 0")
def orders_silver():
    orders_df = process_orders()
    return orders_df

@dp.table
@dp.expect_or_drop("valid_quantity", "quantity <= 0")
def orders_quarantine():
    orders_df = process_orders()
    return orders_df


rules = {
    "recent_updates":"updated >= '2020-01-01'",
    "valid_price":"price between 0 and 100",
    "valid_id":"book_id is not Null"
}

quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

@dp.temporary_view
@dp.expect_all(rules)
def books_raw():
    books_schema = "book_id string, title string, author string, price double, updated timestamp"
    return (
        spark.readStream
             .table("bronze")
             .filter("topic = 'books'")
             .select(F.from_json(F.unbase64(F.col("value")).cast("string"), books_schema).alias("v"))
             .select("v.*")
             .withColumn("is_quarantined",F.expr(quarantine_rules))
    )

    
