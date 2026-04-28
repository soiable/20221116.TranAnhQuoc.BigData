
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col


# 1. CẤU HÌNH MÔI TRƯỜNG HADOOP (DÀNH CHO WINDOWS)

os.environ['HADOOP_HOME'] = r'F:\hadoop'
os.environ['PATH'] += os.pathsep + r'F:\hadoop\bin'


# 2. KHỞI TẠO SPARK SESSION

spark = SparkSession.builder \
    .appName("KafkaStreaming_PopularPages") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")
print("🔥 Spark Streaming is starting and connecting to Kafka...")


# 3. ĐỌC LUỒNG DỮ LIỆU TỪ KAFKA (Topic: pageviews)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pageviews") \
    .load()


data = df.selectExpr("CAST(value AS STRING)")


parsed = data.select(
    split(col("value"), " ").getItem(0).alias("lang"),
    split(col("value"), " ").getItem(1).alias("page"),
    split(col("value"), " ").getItem(2).cast("int").alias("views")
)


# 4. TÍNH TOÁN CỘNG DỒN (AGGREGATION)

result = parsed.groupBy("page").sum("views") \
    .withColumnRenamed("sum(views)", "total_views")


# 5. GHI KẾT QUẢ VÀO KAFKA (Topic: popular_pages)


kafka_output = result.selectExpr(
    "CAST(page AS STRING) AS key",
    "CAST(total_views AS STRING) AS value"
)


query = kafka_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "popular_pages") \
    .option("checkpointLocation", "file:///F:/tmp/checkpoint_popular_pages_v3") \
    .outputMode("update") \
    .start()

print("✅ Streaming is running. Waiting for data...")
query.awaitTermination()
