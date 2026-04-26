from kafka import KafkaProducer
import gzip
import time

# ==============================
# CONFIG
# ==============================
KAFKA_TOPIC = "pageviews"
KAFKA_SERVER = "localhost:9092"
file_path = "../data/pageviews-20260101-000000.gz"   # <-- sửa nếu file nằm chỗ khác
DELAY = 0.01                # thời gian delay giữa mỗi record

# ==============================
# CREATE PRODUCER
# ==============================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: v.encode("utf-8")
)

print("🚀 Start streaming data to Kafka...")

# ==============================
# READ .GZ FILE & SEND TO KAFKA
# ==============================
try:
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()

            # bỏ dòng rỗng
            if not line:
                continue

            # gửi vào Kafka
            producer.send(KAFKA_TOPIC, value=line)

            # log ra để debug
            if i % 100 == 0:
                print(f"Sent {i} records...")

            # giả lập streaming
            time.sleep(DELAY)

except FileNotFoundError:
    print(f"❌ File not found: {file_path}")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    producer.flush()
    producer.close()
    print("✅ Finished streaming data.")
