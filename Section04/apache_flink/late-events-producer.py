import json
import random
import textwrap
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer

def generate_order():
    order_id = f"order-{random.randint(1000, 9999)}"
    customer_id = f"customer-{random.randint(1, 10)}"
    product_id = f"product-{random.randint(1, 200)}"
    quantity = random.randint(1, 5)
    price = round(random.uniform(5.0, 100.0), 2)
    current_time = datetime.now()

    # 20% заказов будут с опозданием (от 1 до 2 минут)
    if random.random() < 0.2:
        late_by = random.randint(60, 120)
        event_time = current_time - timedelta(seconds=late_by)
        is_late = True
    else:
        event_time = current_time
        is_late = False

    order_event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "order_time": event_time.isoformat(),
        "processing_time": current_time.isoformat(),  # Время обработки
        "is_late": is_late  # Флаг опоздания
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
    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "order-producer-late"
    }

    producer = Producer(config)
    
    # Название топика
    topic = 'orders-late'
    
    # Количество заказов для отправки
    orders_count = 9999
    
    print(f"📦 Начинаем отправку {orders_count} заказов в топик '{topic}'...")
    print("=" * 70)
    print("📋 Особенности заказов:")
    print("   • 20% заказов будут с опозданием (задержка 1-2 минуты)")
    print("   • Добавлены поля: processing_time, is_late")
    print("=" * 70)
    
    # Статистика
    late_orders = 0
    on_time_orders = 0
    total_amount = 0
    
    try:
        for i in range(orders_count):
            # Генерируем заказ
            order = generate_order()
            
            # Определяем статус (используем поле is_late из order)
            status = "⏰ ОПОЗДАЛ" if order['is_late'] else "✅ ВОВРЕМЯ"
            
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
            if order['is_late']:
                late_orders += 1
            else:
                on_time_orders += 1
            total_amount += order['quantity'] * order['price']
            
            # Вывод информации об отправке
            order_time = datetime.fromisoformat(order['order_time'])
            processing_time = datetime.fromisoformat(order['processing_time'])
            delay_seconds = (processing_time - order_time).total_seconds()
            
            print(f"{status} Заказ {order['order_id']} | "
                  f"Клиент: {order['customer_id']} | "
                  f"Сумма: ${order['quantity'] * order['price']:.2f} | "
                  f"Задержка: {delay_seconds:.0f} сек.")
            
            # Вызываем коллбэки для обработки ожидающих сообщений
            producer.poll(0)
            
            # Небольшая пауза между отправками
            time.sleep(0.3)
        
        # Ожидаем, пока все сообщения будут доставлены
        print("=" * 70)
        print("⏳ Ожидание доставки всех сообщений...")
        producer.flush()
        
        # Выводим статистику
        print("\n📊 СТАТИСТИКА ЗАКАЗОВ:")
        print("-" * 70)
        print(f"   Всего заказов: {orders_count}")
        print(f"   Заказов вовремя: {on_time_orders} ({on_time_orders/orders_count*100:.1f}%)")
        print(f"   Опоздавших заказов: {late_orders} ({late_orders/orders_count*100:.1f}%)")
        print(f"   Общая сумма всех заказов: ${total_amount:.2f}")
        print(f"   Средняя сумма заказа: ${total_amount / orders_count:.2f}")
        print("=" * 70)
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