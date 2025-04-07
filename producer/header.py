from kafka import KafkaProducer

# プロデューサー設定
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: v.encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# トピック、キー、値、ヘッダー
topic = 'CustomerCountry'
key = 'Precision Products'
value = 'France'
headers = [('privacy-level', b'YOLO')]  # ← ヘッダーは (key: str, value: bytes) のタプルのリスト

# メッセージ送信
future = producer.send(topic, key=key, value=value, headers=headers)
metadata = future.get(timeout=10)  # メッセージのメタデータを取得

print(f"Message sent to topic {metadata.topic}, partition {metadata.partition}, offset {metadata.offset}")

producer.close()
