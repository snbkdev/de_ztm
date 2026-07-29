import json
import random
import textwrap
import time
from datetime import datetime

from confluent_kafka import Producer

def generate_payment():
    payment_id = f"payment-{random.randint(1000, 9999)}"
    user_id = f"user-{random.randint(1, 50)}"
    merchant_id = f"merchant-{random.randint(1, 20)}"
    amount = round(random.uniform(10.0, 1000.0), 2)
    payment_time = datetime.now().isoformat()

    payment_event = {
        "payment_id": payment_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "payment_time": payment_time
    }

    return payment_event

def delivery_report(err, msg):
    """Callback-функция для подтверждения доставки сообщения"""
    if err is not None:
        print(f'❌ Ошибка доставки платежа: {err}')
    else:
        print(f'✅ Платеж доставлен в топик {msg.topic()} [партиция {msg.partition()}] '
              f'по смещению {msg.offset()}')

def main():
    # Конфигурация продюсера
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'payment-producer'
    }
    
    # Создаем продюсера
    producer = Producer(producer_config)
    
    # Название топика для платежей
    topic = 'payments'
    
    # Количество платежей для отправки
    payments_count = 9999
    
    print(f"💳 Начинаем отправку {payments_count} платежей в топик '{topic}'...")
    print("=" * 50)
    
    try:
        for i in range(payments_count):
            # Генерируем платеж
            payment = generate_payment()
            
            # Ключ сообщения (используем user_id для партиционирования)
            # Это гарантирует, что платежи одного пользователя попадают в одну партицию
            key = payment['user_id']
            
            # Сериализуем платеж в JSON
            value = json.dumps(payment, ensure_ascii=False)
            
            # Асинхронная отправка сообщения
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            
            # Вывод информации об отправке
            print(f"💳 Отправлен платеж {payment['payment_id']} "
                  f"(пользователь: {payment['user_id']}, сумма: ${payment['amount']})")
            
            # Вызываем коллбэки для обработки ожидающих сообщений
            producer.poll(0)
            
            # Пауза между отправками для наглядности
            time.sleep(1)
        
        # Ожидаем, пока все сообщения будут доставлены
        print("=" * 50)
        print("⏳ Ожидание доставки всех платежей...")
        producer.flush()
        print("✅ Все платежи успешно отправлены!")
        
        # Выводим статистику
        total_amount = 0
        print("\n📊 Статистика отправленных платежей:")
        print("-" * 30)
        
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
    finally:
        # Закрываем продюсера
        producer.flush()

if __name__ == "__main__":
    main()