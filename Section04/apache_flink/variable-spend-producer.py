import json
import random
import textwrap
import time
from datetime import datetime

from confluent_kafka import Producer

def generate_order():
    order_id = f"order-{random.randint(1000, 9999)}"
    customer_id = f"customer-{random.randint(1, 200)}"
    product_id_idx = random.randint(1, 10)
    product_id = f"product-{product_id_idx}"
    quantity = random.randint(1, 5)
    price = round(random.uniform(5.0, 100.0), 2)
    order_time = datetime.now().isoformat()

    if product_id_idx > 3:
        quantity = random.randint(1, 5)
    else:
        quantity = random.randint(1, 20)

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
        print(f'❌ Ошибка доставки заказа: {err}')
    else:
        print(f'✅ Заказ доставлен в топик {msg.topic()} [партиция {msg.partition()}] '
              f'по смещению {msg.offset()}')

def main():
    # Конфигурация продюсера
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
    
    print(f"📦 Начинаем отправку {orders_count} заказов в топик '{topic}'...")
    print("=" * 60)
    print("📋 Логика формирования заказов:")
    print("   • Товары product-1,2,3 → оптовые заказы (1-20 шт.)")
    print("   • Товары product-4..10 → розничные заказы (1-5 шт.)")
    print("=" * 60)
    
    # Статистика по типам заказов
    wholesale_count = 0
    retail_count = 0
    total_amount = 0
    
    try:
        for i in range(orders_count):
            # Генерируем заказ
            order = generate_order()
            
            # Определяем тип заказа
            order_type = "Оптовый" if order['quantity'] > 5 else "Розничный"
            
            # Ключ сообщения (используем customer_id для партиционирования)
            key = order['customer_id']
            
            # Сериализуем заказ в JSON
            value = json.dumps(order, ensure_ascii=False)
            
            # Асинхронная отправка сообщения
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            
            # Считаем статистику
            if order_type == "Оптовый":
                wholesale_count += 1
            else:
                retail_count += 1
            total_amount += order['quantity'] * order['price']
            
            # Вывод информации об отправке
            print(f"📤 {order_type} заказ {order['order_id']} | "
                  f"Клиент: {order['customer_id']} | "
                  f"Товар: {order['product_id']} | "
                  f"Кол-во: {order['quantity']} шт. | "
                  f"Сумма: ${order['quantity'] * order['price']:.2f}")
            
            # Вызываем коллбэки для обработки ожидающих сообщений
            producer.poll(0)
            
            # Небольшая пауза между отправками
            time.sleep(0.5)
        
        # Ожидаем, пока все сообщения будут доставлены
        print("=" * 60)
        print("⏳ Ожидание доставки всех сообщений...")
        producer.flush()
        
        # Выводим статистику
        print("\n📊 СТАТИСТИКА ЗАКАЗОВ:")
        print("-" * 60)
        print(f"   Всего заказов: {orders_count}")
        print(f"   Оптовых заказов (>= 6 шт.): {wholesale_count}")
        print(f"   Розничных заказов (1-5 шт.): {retail_count}")
        print(f"   Общая сумма всех заказов: ${total_amount:.2f}")
        print(f"   Средняя сумма заказа: ${total_amount / orders_count:.2f}")
        print("=" * 60)
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