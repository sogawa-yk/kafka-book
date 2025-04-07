from confluent_kafka.avro import AvroProducer
import json

# Avroスキーマの読み込み
with open('customer.avsc', 'r') as f:
    value_schema_str = f.read()

# プロデューサー設定
producer_config = {
    'bootstrap.servers': 'kafka:29092',
    'schema.registry.url': 'http://schema-registry:8081'
}

# AvroProducer作成
avro_producer = AvroProducer(
    config=producer_config,
    default_value_schema=value_schema_str
)

# データ送信
customer_data = {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
}

avro_producer.produce(topic='customer-topic', value=customer_data)
avro_producer.flush()

print("Avro message sent successfully!")
