from kafka import KafkaProducer
from json import dumps
import struct

class Customer:
    def __init__(self, customer_id: int, customer_name: str):
        self.customer_id = customer_id
        self.customer_name = customer_name



def customer_serializer(customer: Customer) -> bytes:
    if customer is None:
        return None

    # IDを4バイトのintでシリアライズ
    customer_id_bytes = struct.pack('>i', customer.customer_id)

    # 名前をUTF-8でエンコード
    if customer.customer_name:
        name_bytes = customer.customer_name.encode('utf-8')
    else:
        name_bytes = b''

    name_length_bytes = struct.pack('>i', len(name_bytes))

    return customer_id_bytes + name_length_bytes + name_bytes


producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],  # Docker network内でのアクセス
    value_serializer=customer_serializer
)

customer = Customer(101, "Alice")
producer.send('customer-topic', value=customer)