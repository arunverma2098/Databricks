from pyspark import pipelines as dp
from pyspark.sql import functions as F

dp.create_streaming_table("books_silver")
dp.create_auto_cdc_flow(
    target = "books_silver",
    source = "books_raw",
    keys = ["book_id"],
    sequence_by= F.col("updated"),
    except_column_list=["updated"],
    stored_as_scd_type=2
)





