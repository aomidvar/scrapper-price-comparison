from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime, FileSystem
from pyflink.table.udf import udf
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, lit, row_interval, concat, substring
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema
from pyflink.common.typeinfo import TypeInformation
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, WindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.timecharacteristic import TimeCharacteristic
from pyflink.datastream.window import TimeWindow
from pyflink.metrics import MetricGroup, Metric
from pyflink.table import Row
from pyflink.typehints import Types
from typing import List, Tuple, Any
import os
import requests
from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# Define MongoDB connection parameters
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_USER = 'testscrapper1'
MONGO_PASS = 'shaylin678*h'

# Define MongoDB database and collection names
DB_NAME = 'good_deals'
COLLECTION_NAME = 'products'

# Define Kafka connection parameters
KAFKA_HOST = 'localhost:9092'
KAFKA_TOPIC = 'products'

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

# Create table environment
t_env = StreamTableEnvironment.create(env)

# Define Kafka source
kafka_props = {
    'bootstrap.servers': KAFKA_HOST,
    'group.id': 'my_group'
}

t_env.connect(Kafka()
              .version('universal')
              .topic(KAFKA_TOPIC)
              .start_from_earliest()
              .properties(kafka_props)
              .deserialization_schema(Json()
                                       .fail_on_missing_field(False)
                                       .ignore_parse_errors()
                                       .schema(DataTypes.ROW([

DataTypes.FIELD('product_url', DataTypes.STRING()),
                                           DataTypes.FIELD('brand',
DataTypes.STRING()),
                                           DataTypes.FIELD('title',
DataTypes.STRING()),

DataTypes.FIELD('price_hight', DataTypes.STRING()),

DataTypes.FIELD('price_low', DataTypes.STRING()),
                                           DataTypes.FIELD('sku',
DataTypes.STRING()),

DataTypes.FIELD('availability', DataTypes.STRING()),
                                           DataTypes.FIELD('store_id',
DataTypes.STRING()),

DataTypes.FIELD('category_id', DataTypes.STRING()),
                                           DataTypes.FIELD('date',
DataTypes.STRING())
                                       ]))
              ) \
    .with_format(Json()
                 .fail_on_missing_field(False)
                 .ignore_parse_errors()
                 .schema(DataTypes.ROW([
                     DataTypes.FIELD('product_url', DataTypes.STRING()),
                     DataTypes.FIELD('brand', DataTypes.STRING()),
                     DataTypes.FIELD('title', DataTypes.STRING()),
                     DataTypes.FIELD('price_hight', DataTypes.STRING()),
                     DataTypes.FIELD('price_low', DataTypes.STRING()),
                     DataTypes.FIELD('sku', DataTypes.STRING()),
                     DataTypes.FIELD('availability', DataTypes.STRING()),
                     DataTypes.FIELD('store_id', DataTypes.STRING()),
                     DataTypes.FIELD('category_id', DataTypes.STRING()),
                     DataTypes.FIELD('date', DataTypes.STRING())
                 ]))
                 .for_produced_schema(DataTypes.ROW([
                     DataTypes.FIELD('product_url', DataTypes.STRING()),
                     DataTypes.FIELD('brand', DataTypes.STRING()),
                     DataTypes.FIELD('title', DataTypes.STRING()),
                     DataTypes.FIELD('price_hight', DataTypes

PRODUCT_ID = "my_product_id"
WINDOW_SIZE_WEEK = 60 * 24 * 7 # 1 week in minutes
WINDOW_SIZE_MONTH = 60 * 24 * 30 # 1 month in minutes
WINDOW_SIZE_YEAR = 60 * 24 * 365 # 1 year in minutes

class DiscountedPriceWindowFunction(WindowFunction):

    def __init__(self, window_size):
        self.window_size = window_size

    def apply(self, key, window, input_data, collector):
        current_time = datetime.datetime.now()
        discounted_prices = list(input_data)
        discounted_prices.sort(key=lambda x: x[0])
        latest_price_high = discounted_prices[-1][1]
        min_price_high = min(p[1] for p in discounted_prices)
        max_price_high = max(p[1] for p in discounted_prices)
        historical_mean = sum(p[1] for p in discounted_prices) /
len(discounted_prices)
        discount = (latest_price_high - min_price_high) / latest_price_high
        good_deal = False
        if discount >= 0.2 and latest_price_high <= historical_mean:
            good_deal = True
        # emit result
        collector.collect(Row(current_time, discount, min_price_high,
max_price_high, historical_mean, good_deal))

class HistoricalPriceProcessFunction(KeyedProcessFunction):

    def __init__(self, window_size):
        self.window_size = window_size
        self.state_desc = ValueStateDescriptor("discounted_prices",
Types.LIST(Types.TUPLE([Types.LOCAL_DATE_TIME, Types.FLOAT()])))

    def process_element(self, value, ctx, collector):
        input_data = ctx.get_state(self.state_desc)
        if input_data is None:
            input_data = []
        input_data.append((value[0], value[1]))
        if len(input_data) > self.window_size:
            input_data = input_data[-self.window_size:]
        ctx.get_state(self.state_desc).update(input_data)
        collector.collect((value[0], value[1]))

    def close(self):
        if self.state_desc in
self.get_runtime_context().get_state_backend().get_registered_state_names():
            self.get_runtime_context().get_state_backend().get_partitioned_state(self.state_desc).clear()
        super().close()

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

# configure Kafka consumer
consumer_props = {"bootstrap.servers": "kafka:9092", "group.id": "my_group"}
consumer = FlinkKafkaConsumer("KafkaTopicIN", SimpleStringSchema(),
consumer_props)
consumer.set_start_from_earliest()
stream = env.add_source(consumer).map(lambda msg:
json.loads(msg)).filter(lambda obj: obj["product_id"] == PRODUCT

# Define user-defined function to determine good deals based on
discounted price between high and low price
@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT()],
result_type=DataTypes.BOOLEAN())
def is_good_deal(high_price, low_price):
    return (high_price - low_price) / high_price > 0.2

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

# Create table environment
t_env = StreamTableEnvironment.create(env)

# Define Kafka source
kafka_props = {
    'bootstrap.servers': KAFKA_HOST,
    'group.id': 'my_group'
}

t_env.connect(Kafka()
              .version('universal')
              .topic(KAFKA_TOPIC)
              .start_from_earliest()
              .properties(kafka_props)
              .deserialization_schema(Json()
                                       .fail_on_missing_field(False)
                                       .ignore_parse_errors()
                                       .schema(DataTypes.ROW([

DataTypes.FIELD('product_url', DataTypes.STRING()),
                                           DataTypes.FIELD('brand',
DataTypes.STRING()),
                                           DataTypes.FIELD('title',
DataTypes.STRING()),

DataTypes.FIELD('price_hight', DataTypes.STRING()),

DataTypes.FIELD('price_low', DataTypes.STRING()),
                                           DataTypes.FIELD('sku',
DataTypes.STRING()),

DataTypes.FIELD('availability', DataTypes.STRING()),
                                           DataTypes.FIELD('store_id',
DataTypes.STRING()),

DataTypes.FIELD('category_id', DataTypes.STRING()),
                                           DataTypes.FIELD('date',
DataTypes.STRING())
                                       ]))
              ) \
    .with_format(Json()
                 .fail_on_missing_field(False)
                 .ignore_parse_errors()
                 .schema(DataTypes.ROW([
                     DataTypes.FIELD('product_url', DataTypes.STRING()),
                     DataTypes.FIELD('brand', DataTypes.STRING()),
                     DataTypes.FIELD('title', DataTypes.STRING()),
                     DataTypes.FIELD('price_hight', DataTypes.STRING()),
                     DataTypes.FIELD('price_low', DataTypes.STRING()),
                     DataTypes.FIELD('sku', DataTypes.STRING()),
                     DataTypes.FIELD('availability', DataTypes.STRING()),
                     DataTypes.FIELD('store_id', DataTypes.STRING()),
                     DataTypes.FIELD('category_id', DataTypes.STRING()),
                     DataTypes.FIELD('date', DataTypes.STRING())
                 ]))
                 .for_produced_schema(DataTypes.ROW([
                     DataTypes.FIELD('product_url', DataTypes.STRING()),
                     DataTypes.FIELD('brand', DataTypes.STRING()),
                     DataTypes.FIELD('title', DataTypes.STRING()),
                     DataTypes.FIELD('price_hight', DataTypes

# define a Flink job to perform the "good deal" analysis


job_def = {
    "entryClass": "com.example.GoodDealJob",
    "programArgs": [
        "--product_id", "",
        "--timestamp", "",
        "--price_high", ""
    ]
}

@app.route('/good_deal', methods=['GET'])
def good_deal():
    # parse request parameters
    product_id = request.args.get('product_id')
    timestamp = request.args.get('timestamp')
    price_high = request.args.get('price_high')

    # submit the Flink job
    job_def['programArgs'][1] = product_id
    job_def['programArgs'][3] = timestamp
    job_def['programArgs'][5] = price_high
    response = requests.post(os.environ['FLINK_ENDPOINT'] +
'/jars/upload', files={'jarfile': open('good-deal-job.jar', 'rb')})
    jar_id = response.json()['filename'].replace('.jar', '')
    response = requests.post(os.environ['FLINK_ENDPOINT'] + '/jobs/' +
jar_id + '/run', json=job_def)

    # wait for the job to finish
    job_id = response.json()['jobid']
    response = requests.get(os.environ['FLINK_ENDPOINT'] + '/jobs/' + job_id)
    while response.json()['state'] != 'FINISHED':
        response = requests.get(os.environ['FLINK_ENDPOINT'] +
'/jobs/' + job_id)

    # get the result from the MongoDB collection
    good_deals = db.good_deals
    result = good_deals.find_one({'product_id': product_id,
'timestamp': timestamp})
    good_deal = result['good_deal']
    return jsonify({'product_id': product_id, 'timestamp': timestamp,
'good_deal': good_deal})