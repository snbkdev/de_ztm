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

    # 20% платежей могут быть крупными (до $10000)
    if random.randint(1, 10) < 2:
        amount = round(random.uniform(10.0, 10000.0), 2)
        is_large = True
    else:
        amount = round(random.uniform(10.0, 1000.0), 2)
        is_large = False
    
    payment_time = datetime.now().isoformat()

    payment_event = {
        "payment_id": payment_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "payment_time": payment_time,
        "is_large": is_large  # Флаг крупного платежа
    }
    return payment_event

def delivery_callback(err, msg):
    """Callback-функция для подтверждения доставки сообщения"""
    if err:
        print(f'❌ Ошибка доставки платежа: {err}')
    else:
        print(f'✅ Платеж доставлен в топик {msg.topic()} [партиция {msg.partition()}] '
              f'по смещению {msg.offset()} | Ключ: {msg.key().decode("utf-8") if msg.key() else "None"}')

def main():
    # Конфигурация продюсера
    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "payment-producer"
    }

    # Создаем продюсера
    producer = Producer(config)

    topic = "payments"
    
    # Количество платежей для отправки
    payments_count = 9999
    
    print(f"💳 Начинаем отправку {payments_count} платежей в топик '{topic}'...")
    print("=" * 70)
    print("📋 Особенности платежей:")
    print("   • 20% платежей - крупные (сумма до $10,000)")
    print("   • 80% платежей - обычные (сумма до $1,000)")
    print("=" * 70)
    
    # Статистика
    large_payments = 0
    normal_payments = 0
    total_amount = 0
    max_amount = 0
    min_amount = float('inf')
    
    try:
        for i in range(payments_count):
            # Генерируем платеж
            payment = generate_payment()
            
            # Ключ сообщения (используем user_id для партиционирования)
            key = payment['user_id']
            
            # Сериализуем платеж в JSON
            value = json.dumps(payment, ensure_ascii=False)
            
            # Асинхронная отправка сообщения
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_callback
            )
            
            # Считаем статистику
            if payment['is_large']:
                large_payments += 1
                status = "🔴 КРУПНЫЙ"
            else:
                normal_payments += 1
                status = "🟢 Обычный"
            
            total_amount += payment['amount']
            max_amount = max(max_amount, payment['amount'])
            min_amount = min(min_amount, payment['amount'])
            
            # Вывод информации об отправке
            print(f"{status} Платеж {payment['payment_id']} | "
                  f"Пользователь: {payment['user_id']} | "
                  f"Сумма: ${payment['amount']:.2f} | "
                  f"Торговец: {payment['merchant_id']}")
            
            # Вызываем коллбэки для обработки ожидающих сообщений
            producer.poll(0)
            
            # Пауза между отправками
            time.sleep(0.5)
        
        # Ожидаем, пока все сообщения будут доставлены
        print("=" * 70)
        print("⏳ Ожидание доставки всех платежей...")
        producer.flush()
        
        # Выводим статистику
        print("\n📊 СТАТИСТИКА ПЛАТЕЖЕЙ:")
        print("-" * 70)
        print(f"   Всего платежей: {payments_count}")
        print(f"   Крупных платежей (> $1000): {large_payments} ({large_payments/payments_count*100:.1f}%)")
        print(f"   Обычных платежей: {normal_payments} ({normal_payments/payments_count*100:.1f}%)")
        print(f"   Общая сумма: ${total_amount:.2f}")
        print(f"   Средняя сумма: ${total_amount / payments_count:.2f}")
        print(f"   Максимальная сумма: ${max_amount:.2f}")
        print(f"   Минимальная сумма: ${min_amount:.2f}")
        print("=" * 70)
        print("✅ Все платежи успешно отправлены!")
        
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
    finally:
        # Закрываем продюсера
        producer.flush()

if __name__ == "__main__":
    main()