from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: str(v).encode('utf-8')
)

future_list = []

print("=== キーなし送信 ===")
for i in range(10):
    future = producer.send('test-topic', value=f"no-key-msg-{i}")
    future_list.append(future)


for future in future_list:
    record_metadata = future.get(timeout=10)
    print(f"Message sent to partition: {record_metadata.partition}, offset: {record_metadata.offset}")

producer.flush()

print("\n=== キー付き送信 ===")

key = b"user123"
future_list = []

for i in range(10):
    future = producer.send('test-topic', key=key, value=f"keyed-msg-{i}".encode())
    future_list.append(future)

for future in future_list:
    record_metadata = future.get(timeout=10)
    print(f"Message with key '{key.decode()}' sent to partition: {record_metadata.partition}, offset: {record_metadata.offset}")

producer.flush()