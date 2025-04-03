from kafka import KafkaConsumer
from json import loads

# Kafkaコンシューマーの設定
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['kafka:29092'],  # Docker network内でのアクセス
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# メッセージ受信のループ
print("Waiting for messages...")
for message in consumer:
    print(f"Received: {message.value}")
