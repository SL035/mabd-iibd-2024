# collector/consumer.py
from confluent_kafka import Consumer, KafkaException
import json
import sys

def main():
    conf = {
    'bootstrap.servers': 'redpanda:9092',
    'group.id': 'collector-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'PLAINTEXT',
    'api.version.request': True,
    'broker.version.fallback': '3.0.0',  # ← КЛЮЧЕВОЙ ПАРАМЕТР
    'api.version.fallback.ms': 0
    }

    consumer = Consumer(conf)
    topic = 'events'
    consumer.subscribe([topic])

    print("Сборщик запущен. Ожидание событий из Redpanda...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            event = json.loads(msg.value().decode('utf-8'))
            print(f"✅Получено: {event['event_type']} | user={event['user_id']} | ts={event['timestamp']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    from confluent_kafka.error import KafkaError
    main()