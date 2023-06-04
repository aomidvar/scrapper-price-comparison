from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import json

# Kafka consumer configuration
bootstrap_servers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
sasl_plain_username = "RFU2BCEDGB2UJRWD"
sasl_plain_password = "rjOrdR009uQUx8F67ObMn6on0IAgEtSunMFIKR2KK17SVJqQ1gh+9CmTT9NXiAjp"
group_id = "my-consumer-group"
auto_offset_reset = "earliest"

# Define the Kafka topic to read from
topic_in = 'KafkaTopicIN'
topic_out = 'KafkaTopicOUT'

# Define the schema for the messages
schema = StructType([
    StructField("availability", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("price_height", FloatType(), True),
    StructField("price_low", StringType(), True),
    StructField("product_url", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("title", StringType(), True)
])

# MongoDB configuration parameters
mongodb_uri = "mongodb+srv://scrapper:shaylin1396*M@cluster0.cco0jwv.mongodb.net/?retryWrites=true&w=majority"
mongodb_database = "deals"
mongodb_collection = "good_deals"

# Window duration in seconds for each timeframe
window_week = timedelta(weeks=1).total_seconds()
window_month = timedelta(days=30).total_seconds()
window_year = timedelta(days=365).total_seconds()
window_historical = float("inf")

# Create a Kafka consumer with the provided configuration
consumer = KafkaConsumer(
    topic_in,
    bootstrap_servers=bootstrap_servers,
    security_protocol=security_protocol,
    sasl_mechanism=sasl_mechanism,
    sasl_plain_username=sasl_plain_username,
    sasl_plain_password=sasl_plain_password,
    group_id=group_id,
    auto_offset_reset=auto_offset_reset
)

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaWindowAnalysis") \
    .getOrCreate()

# Create a MongoDB client and connect to the database and collection
client = MongoClient(mongodb_uri)
db = client[mongodb_database]
collection = db[mongodb_collection]

# Create the database and collection if they don't exist
if mongodb_database not in client.list_database_names():
    db = client[mongodb_database]
    collection = db.create_collection(mongodb_collection)

# Process messages from Kafka
for message in consumer:
    # Decode the message value
    message_value = message.value.decode('utf-8')

    # Parse the JSON message
    json_data = json.loads(message_value)

    # Create a DataFrame from the JSON message
    df = spark.createDataFrame([json_data], schema)

    # Convert the date column to timestamp
    df = df.withColumn("date", to_timestamp(df["date"]))

    # Define the time-based window specification
    windowSpec = Window.orderBy("date").rowsBetween(-window_historical, 0)

    # Add columns for each timeframe to indicate whether the price is good
    df = df.withColumn("is_good_price_week", df["price_height"].between(
        min(df["price_height"]).over(windowSpec),
        max(df["price_height"]).over(windowSpec)
    ))
    df = df.withColumn("is_good_price_month", df["price_height"].between(
        min(df["price_height"]).over(windowSpec),
        max(df["price_height"]).over(windowSpec)
    ))
    df = df.withColumn("is_good_price_year", df["price_height"].between(
        min(df["price_height"]).over(windowSpec),
        max(df["price_height"]).over(windowSpec)
    ))
    df = df.withColumn("is_good_price_historical", df["price_height"].between(
        min(df["price_height"]).over(windowSpec),
        max(df["price_height"]).over(windowSpec)
    ))

    # Extract the required fields and convert to JSON
    df = df.select(
        "product_url", "brand", "title", "price_height",
        "is_good_price_week", "is_good_price_month",
        "is_good_price_year", "is_good_price_historical"
    )
    json_data = df.toJSON().first()

    # Store the data in MongoDB
    collection.insert_one(json.loads(json_data))

    # Publish the good deals to Kafka
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(topic_out, json_data.encode('utf-8'))
    producer.flush()
