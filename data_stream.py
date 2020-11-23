from pyspark.sql.types import StructField, StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import json

schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True),
])

def run_spark_job(spark):

    df = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers','localhost:9092') \
            .option('subscribe', 'POLICE_CALLS') \
            .option('startingOffsets', 'earliest') \
            .option('maxOffsetsPerTrigger', 8000) \
            .option('stopGracefullyOnShutdown', "true") \
            .load()

    df.printSchema()

    kafka_df = df.select(df.value.cast('string'))

    service_table = kafka_df.select(from_json(col('value'), schema).alias("DF")).select("DF.*")

    distinct_table = service_table.select('original_crime_type_name', 'disposition', 'call_date_time').distinct()

    agg_df = distinct_table \
        .dropna() \
        .select('original_crime_type_name') \
        .groupby('original_crime_type_name') \
        .agg({'original_crime_type_name': 'count'})\
        .orderBy("count(original_crime_type_name)", ascending=False)

    query = agg_df.writeStream.format('console')\
        .trigger(processingTime='30 seconds')\
        .outputMode('complete')\
        .option("truncate", "false")\
        .start()

    query.awaitTermination()

    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)
    
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = agg_df.join(radio_code_df, 'disposition', 'left_outer')
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 4) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
