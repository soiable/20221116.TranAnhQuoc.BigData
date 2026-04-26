#
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split, col
#
# # Fix Hadoop trên Windows
# os.environ['HADOOP_HOME'] = r'F:\hadoop'
# os.environ['PATH'] += os.pathsep + r'F:\hadoop\bin'
#
# # Khởi tạo Spark
# spark = SparkSession.builder \
#     .appName("KafkaStreaming") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .getOrCreate()
#
# spark.sparkContext.setLogLevel("ERROR")
#
# print("🔥 Spark Streaming is starting...")
#
# # 1. ĐỌC DỮ LIỆU TỪ KAFKA (Topic: pageviews)
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "pageviews") \
#     .load()
#
# # 2. XỬ LÝ DỮ LIỆU THÔ
# data = df.selectExpr("CAST(value AS STRING)")
# parsed = data.select(
#     split(col("value"), " ").getItem(0).alias("lang"),
#     split(col("value"), " ").getItem(1).alias("page"),
#     split(col("value"), " ").getItem(2).cast("int").alias("views")
# )
#
# # 3. TÍNH TOÁN CỘNG DỒN (AGGREGATION)
# result = parsed.groupBy("page").sum("views") \
#     .withColumnRenamed("sum(views)", "total_views")
#
# # 4. CHUYỂN ĐỔI SANG ĐỊNH DẠNG KAFKA (key, value)
# kafka_output = result.selectExpr("CAST(page AS STRING) AS key", "CAST(total_views AS STRING) AS value")
#
# # 5. GHI KẾT QUẢ VÀO KAFKA (Topic: popular_pages)
# query = kafka_output.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "popular_pages") \
#     .option("checkpointLocation", "./checkpoint_pages") \
#     .outputMode("update") \
#     .start()
#
# query.awaitTermination()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG HADOOP (DÀNH CHO WINDOWS)
# ==========================================
# Lưu ý: Sửa lại đường dẫn này nếu thư mục hadoop của bạn nằm ở ổ đĩa khác
os.environ['HADOOP_HOME'] = r'F:\hadoop'
os.environ['PATH'] += os.pathsep + r'F:\hadoop\bin'

# ==========================================
# 2. KHỞI TẠO SPARK SESSION
# ==========================================
spark = SparkSession.builder \
    .appName("KafkaStreaming_PopularPages") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Giảm bớt log rác trên terminal để dễ nhìn
spark.sparkContext.setLogLevel("ERROR")
print("🔥 Spark Streaming is starting and connecting to Kafka...")

# ==========================================
# 3. ĐỌC LUỒNG DỮ LIỆU TỪ KAFKA (Topic: pageviews)
# ==========================================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pageviews") \
    .load()

# Chuyển đổi dữ liệu byte của Kafka sang String
data = df.selectExpr("CAST(value AS STRING)")

# Tách chuỗi dữ liệu (Format từ file: "lang page views")
parsed = data.select(
    split(col("value"), " ").getItem(0).alias("lang"),
    split(col("value"), " ").getItem(1).alias("page"),
    split(col("value"), " ").getItem(2).cast("int").alias("views")
)

# ==========================================
# 4. TÍNH TOÁN CỘNG DỒN (AGGREGATION)
# ==========================================
# Gom nhóm theo trang (page) và tính tổng lượt xem (views)
result = parsed.groupBy("page").sum("views") \
    .withColumnRenamed("sum(views)", "total_views")

# ==========================================
# 5. GHI KẾT QUẢ VÀO KAFKA (Topic: popular_pages)
# ==========================================
# Kafka yêu cầu dữ liệu gửi vào phải có định dạng (key, value) bằng String
kafka_output = result.selectExpr(
    "CAST(page AS STRING) AS key",
    "CAST(total_views AS STRING) AS value"
)

# Đẩy luồng dữ liệu đã tính toán ra Kafka cho Dashboard đọc
# Chế độ "update": Chỉ gửi đi những trang có sự thay đổi lượt xem
query = kafka_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "popular_pages") \
    .option("checkpointLocation", "file:///F:/tmp/checkpoint_popular_pages_v3") \
    .outputMode("update") \
    .start()

print("✅ Streaming is running. Waiting for data...")
query.awaitTermination()