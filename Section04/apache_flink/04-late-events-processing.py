import json
from dataclasses import dataclass
from datetime import datetime

from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction

@dataclass
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    price: float
    order_time: str


def parse_order(json_str) -> Order:
    try:
        data = json.loads(json_str)
        return Order(
            order_id=data.get("order_id", "unknown"),
            customer_id=data.get("customer_id", "unknown"),
            product_id=data.get("product_id", "unknown"),
            quantity=data.get("quantity", 0),
            price=float(data.get("price", 0.0)),
            order_time=data.get("order_time", "unknown")
        )
    except Exception as e:
        print(f"Error parsing: {e}")
        return Order("unknown", "unknown", "unknown", 0, 0.0, "unknown")


class AggregateWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        total_quantity = 0
        total_sum = 0.0

        for element in elements:
            total_quantity += element.quantity
            total_sum += element.quantity * element.price

        result = {
            "product_id": key,
            "total_quantity": total_quantity,
            "total_spent": round(total_sum, 2),
            "window_start": datetime.utcfromtimestamp(
                context.window().start / 1000
            ).isoformat(),
            "window_end": datetime.utcfromtimestamp(
                context.window().end / 1000
            ).isoformat(),
        }

        return [json.dumps(result)]


def main():
    print("Starting Flink job...")
    
    # Создаем окружение
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    
    # НЕ добавляем JAR через env.add_jars() - будем передавать через командную строку

    # Создаем Kafka источник
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("orders-late")
        .set_group_id("eventtime-demo")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Читаем из Kafka
    orders_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    )

    # Парсим заказы
    parsed_stream = orders_stream.map(parse_order, Types.PICKLED_BYTE_ARRAY())

    # Агрегируем
    processed_stream = (
        parsed_stream
        .key_by(lambda x: x.product_id)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
        .process(AggregateWindowFunction(), Types.STRING())
    )

    # Выводим результат
    processed_stream.print()

    # Выполняем задачу
    print("Executing Flink job...")
    env.execute("Window-based aggregation for late orders")


if __name__ == "__main__":
    main()