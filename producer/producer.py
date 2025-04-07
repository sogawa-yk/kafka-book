from kafka import KafkaProducer
from json import dumps
import time

# Kafkaブローカーの設定
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],  # Docker network内でのアクセス
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# メッセージ送信のサンプル
try:
    for i in range(100):
        message = {'number': i}
        producer.send('test-topic', value=message).get()
        print(f"Sent: {message}")
finally:
    producer.close()
