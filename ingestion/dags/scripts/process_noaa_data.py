import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import IntegerType, FloatType, StringType


def convert_damage(damage_str):
    if damage_str is None or damage_str.strip() == "":
        return 0

    damage_str = damage_str.upper().strip()

    if damage_str.endswith("K"):
        return int(float(damage_str[:-1]) * 1_000)
    elif damage_str.endswith("M"):
        return int(float(damage_str[:-1]) * 1_000_000)
    elif damage_str.endswith("B"):
        return int(float(damage_str[:-1]) * 1_000_000_000)
    else:
        return int(float(damage_str))


convert_damage_udf = udf(convert_damage, IntegerType())


def process_noaa_data(input_path, output_path):
    spark = SparkSession.builder.master("local[*]").appName("NoaaProcessing").getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    if "storms" in input_path:
        df_cleaned = df.select(
            col("EVENT_ID").cast(StringType()).alias("event_id"),
            col("STATE").cast(StringType()).alias("state"),
            col("EVENT_TYPE").cast(StringType()).alias("event_type"),
            to_timestamp(col("BEGIN_DATE_TIME"), "d-MMM-yy HH:mm:ss").alias("event_start_date"),
            to_timestamp(col("END_DATE_TIME"), "d-MMM-yy HH:mm:ss").alias("event_end_date"),
            col("INJURIES_DIRECT").cast(IntegerType()).alias("injuries_direct"),
            col("INJURIES_INDIRECT").cast(IntegerType()).alias("injuries_indirect"),
            col("DEATHS_DIRECT").cast(IntegerType()).alias("deaths_direct"),
            col("DEATHS_INDIRECT").cast(IntegerType()).alias("deaths_indirect"),
            convert_damage_udf(col("DAMAGE_PROPERTY")).alias("damage_property"),
            convert_damage_udf(col("DAMAGE_CROPS")).alias("damage_crops"),
            col("SOURCE").cast(StringType()).alias("source"),
            col("MAGNITUDE").cast(FloatType()).alias("magnitude"),
            col("MAGNITUDE_TYPE").cast(StringType()).alias("magnitude_type")
        )
    else:
        df_cleaned = df.select(
            col("FATALITY_ID").cast(StringType()).alias("fatality_id"),
            col("EVENT_ID").cast(StringType()).alias("event_id"),
            col("FATALITY_TYPE").cast(StringType()).alias("fatality_type"),
            to_timestamp(col("FATALITY_DATE"), "dd/MM/yyyy HH:mm:ss").alias("fatality_date"),
            col("FATALITY_AGE").cast(IntegerType()).alias("fatality_age"),
            col("FATALITY_SEX").cast(StringType()).alias("fatality_sex"),
            col("FATALITY_LOCATION").cast(StringType()).alias("fatality_location")
        )

    df_cleaned.repartition(4).write.format("parquet").mode("overwrite").save(output_path)

    spark.stop()


input_path = sys.argv[1]
output_path = sys.argv[2]

process_noaa_data(input_path, output_path)
