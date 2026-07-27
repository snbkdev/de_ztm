import base64
import json
from decimal import Decimal

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-price-consumer",
    "auto.offset.reset": "earliest",
}

def main():
    consumer = Consumer(consumer_config)

    topic = "postgres-.public.orders"
    consumer.subscribe([topic])

    try:
        print(f"Consuming messages from topic '{topic}'")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Error encountered: {msg.error()}")
                continue

            process_message(msg)
            
    finally:
        consumer.close()

def process_message(msg):
    try:
        value = msg.value()
        if not value:
            print("Empty message received")
            return
            
        order = json.loads(value.decode("utf-8"))
        
        # Структура Debezium: поля на верхнем уровне
        # 'before' - состояние до изменения (для UPDATE/DELETE)
        # 'after' - состояние после изменения (для CREATE/UPDATE)
        # 'op' - операция (c=create, u=update, d=delete)
        # 'source' - метаданные источника
        # 'ts_ms' - временная метка
        
        operation = order.get("op")
        
        # Пропускаем DELETE операции
        if operation == "d":
            print("⚠️ DELETE operation received, skipping")
            return
            
        # Получаем данные после операции
        after = order.get("after")
        if not after:
            print("⚠️ No 'after' data in message")
            print(f"   Operation: {operation}")
            print(f"   Available fields: {order.keys()}")
            return
            
        # Получаем total_amount
        total_amount_data = after.get("total_amount")
        if total_amount_data is None:
            print(f"⚠️ 'total_amount' field is missing or null")
            print(f"   Available fields in 'after': {after.keys()}")
            return
            
        # Декодируем decimal из base64
        total_amount = decode_decimal(total_amount_data)
        if total_amount is not None:
            print(f"✅ Received order with total amount={total_amount}")
            
        # Дополнительная информация (опционально)
        order_id = after.get("id")
        print(f"   Order ID: {order_id}, Operation: {operation}")
        
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON: {e}")
        print(f"   Raw message: {value[:200]}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def decode_decimal(encoded_string, scale=2):
    """Декодирует decimal из base64"""
    if not encoded_string:
        return None
        
    if isinstance(encoded_string, (int, float, str)):
        # Если это уже число, просто возвращаем
        if isinstance(encoded_string, (int, float)):
            return Decimal(str(encoded_string))
        # Если строка и не похожа на base64, может быть уже готовым числом
        if encoded_string.isdigit() or encoded_string.replace('.', '').isdigit():
            return Decimal(encoded_string)
            
    try:
        # Декодируем base64
        value_bytes = base64.b64decode(encoded_string)
        unscaled_value = int.from_bytes(value_bytes, byteorder="big", signed=True)
        return Decimal(unscaled_value) / Decimal(10**scale)
    except Exception as e:
        print(f"❌ Failed to decode decimal: {e}")
        print(f"   Encoded value: {encoded_string[:100]}")
        return None

if __name__ == "__main__":
    main()