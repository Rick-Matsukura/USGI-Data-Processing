import warnings
import requests
import pandas as pd
import matplotlib.pyplot as plt
from time import sleep as time_sleep, time as time_time
from confluent_kafka import Producer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, DoubleType, LongType
from datetime import datetime, timedelta
import threading

# Suppress FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'usgs-producer'
}
producer = Producer(producer_conf)

# USGS Earthquake API endpoint
base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"


# Function to fetch earthquake data for a specific time range
def fetch_earthquake_data(starttime, endtime):
    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


# Function to send data to Kafka
def send_to_kafka(data, topic='microbatch-earthquake-data'):
    for feature in data['features']:
        event_data = json.dumps(feature)
        producer.produce(topic, key=str(feature['id']), value=event_data)
        producer.poll(0)
    producer.flush()


# Function to simulate the periodic sending of data
def periodic_data_sender(start_date, end_date, interval_days=30, period_seconds=1):
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=interval_days)
        try:
            data = fetch_earthquake_data(current_date.isoformat(), next_date.isoformat())
            send_to_kafka(data)
            print(f"Sent data from {current_date} to {next_date}")
        except Exception as e:
            print(f"Failed to fetch or send data for {current_date} to {next_date}: {e}")
        finally:
            current_date = next_date
            time_sleep(period_seconds)


# Start the data sending in a separate thread
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
data_sender_thread = threading.Thread(target=periodic_data_sender, args=(start_date, end_date, 30, 1))
data_sender_thread.start()

# Create the Spark Session
spark = (
    SparkSession
    .builder
    .appName("Kafka schema to Spark Dataframe")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
    .config("spark.sql.shuffle.partitions", 4)
    .master("spark://spark-master:7077")
    .getOrCreate()
)

# Create the kafka_df to read from kafka
batch_start_time = time_time()  # Start timing for the batch

kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "microbatch-earthquake-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("max.partition.fetch.bytes", "104857600")
    .option("fetch.max.bytes", "104857600")
    .load()
)

# Define Schema
json_schema = StructType([
    StructField("type", StringType(), True),
    StructField("properties", StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("tz", StringType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", LongType(), True),
        StructField("cdi", DoubleType(), True),
        StructField("mmi", DoubleType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", LongType(), True),
        StructField("sig", LongType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", LongType(), True),
        StructField("dmin", DoubleType(), True),
        StructField("rms", DoubleType(), True),
        StructField("gap", DoubleType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True)
    ]), True),
    StructField("geometry", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True)
    ]), True),
    StructField("id", StringType(), True)
])

# Convert JSON string to Dataframe
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data"))

flattened_df = json_df.select(
    col("data.properties.mag").alias("magnitude"),
    col("data.properties.place").alias("place"),
    col("data.properties.time").alias("time"),
    col("data.properties.updated").alias("updated"),
    col("data.properties.tz").alias("tz"),
    col("data.properties.url").alias("url"),
    col("data.properties.detail").alias("detail"),
    col("data.properties.felt").alias("felt"),
    col("data.properties.cdi").alias("cdi"),
    col("data.properties.mmi").alias("mmi"),
    col("data.properties.alert").alias("alert"),
    col("data.properties.status").alias("status"),
    col("data.properties.tsunami").alias("tsunami"),
    col("data.properties.sig").alias("sig"),
    col("data.properties.net").alias("net"),
    col("data.properties.code").alias("code"),
    col("data.properties.ids").alias("ids"),
    col("data.properties.sources").alias("sources"),
    col("data.properties.types").alias("types"),
    col("data.properties.nst").alias("nst"),
    col("data.properties.dmin").alias("dmin"),
    col("data.properties.rms").alias("rms"),
    col("data.properties.gap").alias("gap"),
    col("data.properties.magType").alias("magType"),
    col("data.properties.type").alias("type"),
    col("data.properties.title").alias("title"),
    col("data.geometry.coordinates")[0].alias("longitude"),
    col("data.geometry.coordinates")[1].alias("latitude"),
    col("data.geometry.coordinates")[2].alias("depth"),
    col("data.id").alias("id")
)


# Display data in custom format and measure latency
def display_in_notebook(batch_df, batch_id):
    global batch_start_time
    pd_df = batch_df.toPandas()
    print(pd_df)

    # Plot
    fig2, ax2 = plt.subplots(figsize=(18, 6))
    scatter = ax2.scatter(pd_df['longitude'], pd_df['latitude'], c=pd_df['magnitude'], cmap='viridis', alpha=0.5,
                          label='Earthquakes')

    # Filter for tsunami events
    tsunami_events = pd_df[pd_df['tsunami'] == 1]

    # Plot tsunami events
    scatter_tsunami = ax2.scatter(tsunami_events['longitude'], tsunami_events['latitude'], c='red', alpha=0.6,
                                  label='Tsunami Events')
    fig2.colorbar(scatter, ax=ax2, label='Magnitude')
    ax2.set_title('Scatter Plot of Earthquake Locations')
    ax2.set_xlabel('Longitude')
    ax2.set_ylabel('Latitude')
    ax2.legend()
    plt.tight_layout()
    plt.show()

    # Display Tsunami Events in a Table
    if not tsunami_events.empty:
        print("Tsunami Events Data:")
        print(tsunami_events[['time', 'place', 'magnitude', 'longitude', 'latitude', 'depth']].to_string(index=False))
    else:
        print("No Tsunami Events Found")

    # Insert a blank line
    print("\n")

    # Record end time for processing time latency
    processing_end_time = time_time()
    processing_time_latency = processing_end_time - batch_start_time
    print(f"Processing Time Latency for Batch {batch_id}: {processing_time_latency:.2f} seconds")

    # Calculate data size
    memory_usage = pd_df.memory_usage(deep=True).sum() / (1024 ** 2)  # Convert bytes to megabytes
    print(f"Data Size for Batch {batch_id}: {memory_usage:.2f} MB")

    # Calculate and print the
    throughput = memory_usage / processing_time_latency  # MB per second
    print(f"Throughput for Batch {batch_id}: {throughput:.2f} MB/s")

    # Insert a blank line
    print("\n")

    # Update the batch start time for the next batch
    batch_start_time = time_time()


# Output data
query = flattened_df.writeStream \
    .foreachBatch(display_in_notebook) \
    .outputMode("append") \
    .trigger(processingTime='1 seconds') \
    .start()

# Wait for new data to arrive
query.awaitTermination()

# Main thread continues here after starting the periodic data sender thread and the Spark Streaming query