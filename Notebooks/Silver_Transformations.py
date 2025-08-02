from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt


############ Schema #####################

"""

schema = StructType([
    StructField("status", StringType(), True),
    StructField("data", StructType([
        StructField("aqi", StringType(), True),
        StructField("idx", StringType(), True),
        StructField("attributions", ArrayType(
            StructType([
                StructField("url", StringType(), True),
                StructField("name", StringType(), True),
                StructField("logo", StringType(), True)
            ])
        ), True),
        StructField("city", StructType([
            StructField("geo", ArrayType(StringType()), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
            StructField("location", StringType(), True)
        ]), True),
        StructField("dominentpol", StringType(), True),
        StructField("iaqi", StructType([
            StructField("co", StructType([StructField("v", StringType(), True)]), True),
            StructField("dew", StructType([StructField("v", StringType(), True)]), True),
            StructField("h", StructType([StructField("v", StringType(), True)]), True),
            StructField("no2", StructType([StructField("v", StringType(), True)]), True),
            StructField("o3", StructType([StructField("v", StringType(), True)]), True),
            StructField("p", StructType([StructField("v", StringType(), True)]), True),
            StructField("pm10", StructType([StructField("v", StringType(), True)]), True),
            StructField("pm25", StructType([StructField("v", StringType(), True)]), True),
            StructField("so2", StructType([StructField("v", StringType(), True)]), True),
            StructField("t", StructType([StructField("v", StringType(), True)]), True),
            StructField("w", StructType([StructField("v", StringType(), True)]), True),
            StructField("wg", StructType([StructField("v", StringType(), True)]), True)
        ]), True),
        StructField("time", StructType([
            StructField("s", StringType(), True),
            StructField("tz", StringType(), True),
            StructField("v", StringType(), True),
            StructField("iso", StringType(), True)
        ]), True),
        StructField("forecast", StructType([
            StructField("daily", StructType([
                StructField("pm10", ArrayType(
                    StructType([
                        StructField("avg", StringType(), True),
                        StructField("day", StringType(), True),
                        StructField("max", StringType(), True),
                        StructField("min", StringType(), True)
                    ])
                ), True),
                StructField("pm25", ArrayType(
                    StructType([
                        StructField("avg", StringType(), True),
                        StructField("day", StringType(), True),
                        StructField("max", StringType(), True),
                        StructField("min", StringType(), True)
                    ])
                ), True),
                StructField("uvi", ArrayType(
                    StructType([
                        StructField("avg", StringType(), True),
                        StructField("day", StringType(), True),
                        StructField("max", StringType(), True),
                        StructField("min", StringType(), True)
                    ])
                ), True)
            ]), True)
        ]), True),
        StructField("debug", StructType([
            StructField("sync", StringType(), True)
        ]), True)
    ]), True)
])


############# RAW DATA INGESTION ##########
@dlt.table(name="AirQuality_Raw")
def ingest_raw_data():
    df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka-azure.servicebus.windows.net:9093")\
    .option("subscribe", "weatherapistream")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.sasl.jaas.config", 
            'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="$ConnectionString" '
            'password="Endpoint=sb://kafka-azure.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=+EglVf4223K/6jUpB3OPZCcHywzoMuekr+AEhIipOc8=";')\
    .option("startingOffsets", "earliest")\
    .load()

    df_json =   df.select(
    from_json(col("value").cast("string"), schema).alias("df_json")
    )
    df_flat = df_json.select(
    col("df_json.data.aqi"),
    col("df_json.data.idx"),
    col("df_json.data.city.name").alias("city_name"),
    col("df_json.data.dominentpol").alias("dominant_pollutant"),
    col("df_json.data.iaqi.co.v").alias("co"),
    col("df_json.data.iaqi.dew.v").alias("dew"),
    col("df_json.data.iaqi.h.v").alias("humidity"),
    col("df_json.data.iaqi.no2.v").alias("no2"),
    col("df_json.data.iaqi.o3.v").alias("o3"),
    col("df_json.data.iaqi.p.v").alias("pressure"),
    col("df_json.data.iaqi.pm10.v").alias("pm10"),
    col("df_json.data.iaqi.pm25.v").alias("pm25"),
    col("df_json.data.iaqi.so2.v").alias("so2"),
    col("df_json.data.iaqi.t.v").alias("temperature"),
    col("df_json.data.iaqi.w.v").alias("wind_speed"),
    col("df_json.data.iaqi.wg.v").alias("wind_gust"),
    col("df_json.data.time.s").alias("time_s")
    )
    df_clean = df_flat.na.drop()
    df_flat.writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/kafka_azure_databricks/bronze/checkpoint")\
    .trigger(once=True)\
    .outputMode("append")\
    .toTable('kafka_azure_databricks.bronze.weatherTable')

    return df_clean """


###### staging airquality table#############

@dlt.table(name="stage_AirQuality")
def stage_AirQuality():
     return spark.readStream.table("kafka_azure_databricks.bronze.weathertable")

###### AirQuality Transformation View ########
@dlt.view(name='Trans_AirQuality')
def trans_AirQuality():
    df=dlt.read_stream('stage_AirQuality')
    return (
        df.withColumn("aqi", col("aqi").cast(IntegerType()))
        .withColumn("idx", col("idx").cast(IntegerType()))
        .withColumn("co",col("co").cast(DoubleType()))
        .withColumn("dew",col("dew").cast(DoubleType()))
        .withColumn("humidity",col("humidity").cast(DoubleType()))
        .withColumn("no2",col("no2").cast(DoubleType()))
        .withColumn("o3",col("o3").cast(DoubleType()))
        .withColumn("pressure",col("pressure").cast(DoubleType()))
        .withColumn("so2",col("so2").cast(DoubleType()))
        .withColumn("pm25",col("pm25").cast(DoubleType()))
        .withColumn("pm10",col("pm10").cast(DoubleType()))
        .withColumn("temperature",col("temperature").cast(DoubleType()))
        .withColumn("wind_speed",col("wind_speed").cast(DoubleType()))
        .withColumn("wind_gust",col("wind_gust").cast(DoubleType()))
        .withColumn("time_s",col("time_s").cast(TimestampType()))
        .withColumn("Modified_Date",current_timestamp())
     )
    
######## silver AirQuality table with Auto CDC ###########  

dlt.create_streaming_table(name="silver_AirQuality")
def silver_AirQuality():
    return dlt.readStream("Trans_AirQuality")

dlt.apply_changes(
    target="silver_airquality",
    source="bronze_airquality",
    keys=["idx"],  
    sequence_by=col("Modified_Date"),  
    stored_as_scd_type = 1  
)
