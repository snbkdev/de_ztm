from confluent_kafka import Consumer, Producer
import sys

def test_kafka():
    try:
        # Тест consumer
        consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        print("Consumer created successfully!")
        consumer.close()
        
        # Тест producer
        producer_config = {
            'bootstrap.servers': 'localhost:9092'
        }
        producer = Producer(producer_config)
        print("Producer created successfully!")
        
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    if test_kafka():
        print("✅ Kafka connection successful!")
    else:
        print("❌ Kafka connection failed!")
        sys.exit(1)