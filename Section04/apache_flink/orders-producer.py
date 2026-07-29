import json
import random
import textwrap
import time

from datetime import datetime
from confluent_kafka import Producer

def generate_order():
    order_id = f"order-{random.randint(1000, 9999)}"
    customer_id = f"customer-{random.randint(1, 200)}"
    product_id = f"product-{random.randint(1, 10)}"
    quantity = random.randint(1, 5)
    price = round(random.uniform(5.0, 100.0), 2)
    order_time = datetime.now().isoformat()

    order_event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "order_time": order_time
    }

    return order_event

def delivery_report(err, msg):
    """Callback-функция для подтверждения доставки сообщения"""
    if err is not None:
        print(f'❌ Ошибка доставки: {err}')
    else:
        print(f'✅ Заказ доставлен в топик {msg.topic()} [партиция {msg.partition()}] '
              f'по смещению {msg.offset()}')

def main():
    # Конфигурация продюсера
    # Замените 'localhost:9092' на адрес вашего брокера
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'order-producer'
    }
    
    # Создаем продюсера
    producer = Producer(producer_config)
    
    # Название топика
    topic = 'orders'
    
    # Количество заказов для отправки
    orders_count = 9999
    
    print(f"🚀 Начинаем отправку {orders_count} заказов в топик '{topic}'...")
    print("=" * 50)
    
    try:
        for i in range(orders_count):
            # Генерируем заказ
            order = generate_order()
            
            # Ключ сообщения (опционально, для партиционирования)
            # Используем order_id как ключ, чтобы заказы с одним ID попадали в одну партицию
            key = order['order_id']
            
            # Сериализуем заказ в JSON
            value = json.dumps(order, ensure_ascii=False)
            
            # Асинхронная отправка сообщения
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            
            print(f"📤 Отправлен заказ {order['order_id']} "
                  f"(товар: {order['product_id']}, кол-во: {order['quantity']})")
            
            # Вызываем коллбэки для обработки ожидающих сообщений
            producer.poll(0)
            
            # Пауза между отправками для наглядности
            time.sleep(1)
        
        # Ожидаем, пока все сообщения будут доставлены
        print("=" * 50)
        print("⏳ Ожидание доставки всех сообщений...")
        producer.flush()
        print("✅ Все заказы успешно отправлены!")
        
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
    finally:
        # Закрываем продюсера
        producer.flush()

if __name__ == "__main__":
    main()