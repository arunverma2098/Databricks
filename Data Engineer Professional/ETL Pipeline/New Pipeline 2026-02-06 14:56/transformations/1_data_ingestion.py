from pyspark import pipelines as dp
from pyspark.sql import functions as F

dataset_path = spark.conf.get("dataset_path")

@dp.table(name = "bronze",
           partition_cols = ["topic", "year-month"],
           table_properties={
               "delta.appendOnly": "true",
               "pipeline.reset.allowed" : "false"
           }
           )
def process_bronze():
    json_schema = "key string, offset long, partition long, timestamp long, topic string, value string"
    bronze_df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", 'json')
                 .schema(json_schema)
                 .load(f"{dataset_path}/kafka-raw-etl/")
                 .withColumn("timestamp", F.from_unixtime(F.col("timestamp")/1000).cast("timestamp"))
                 .withColumn("year-month", F.date_format(F.col("timestamp"),"yyyy-MM"))
                 .withColumn("_src_file", F.col("_metadata.file_name"))
                 .withColumn("load_timestamp", F.current_timestamp())
    )
    return bronze_df

@dp.temporary_view
def country_lookup():
    countries_df = spark.read.json(f"{dataset_path}/country_lookup/")
    return countries_df
