# collector/consumer.py
from confluent_kafka import Consumer, KafkaException
import json
import sys
import clickhouse_connect
from datetime import datetime
import pytz

def main():
    # Kafka
    kafka_conf = {
        'bootstrap.servers': 'redpanda:9092',
        'group.id': 'collector-group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT',
        'api.version.request': True,
        'broker.version.fallback': '3.0.0'
    }
    consumer = Consumer(kafka_conf)
    consumer.subscribe(['events'])

    # ClickHouse
    ch_client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        database='events_db'
    )
    print("📡 Сборщик запущен → запись в ClickHouse...")

    batch = []
    BATCH_SIZE = 100

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
            # Обработка формата ISO с Z (например, "2026-01-22T12:34:56.789Z")
            ts_str = event['timestamp']
            # Убираем лишние символы, если есть (например, +00:00 → Z)
            if ts_str.endswith('Z'):
                ts_str = ts_str[:-1] + '+00:00'
            # Парсим
            event_time = datetime.fromisoformat(ts_str).replace(tzinfo=pytz.UTC)

            batch.append([
                event['event_id'],
                event['user_id'],
                event['event_type'],
                event['product_id'],
                event_time,
                event['session_id'],
                event['value']
            ])

            if len(batch) >= BATCH_SIZE:
                ch_client.insert(
                    'events_db.events',
                    batch,
                    column_names=['event_id', 'user_id', 'event_type', 'product_id', 'timestamp', 'session_id', 'value']
                )
                print(f"Вставлено {len(batch)} событий в ClickHouse")
                batch.clear()

    except KeyboardInterrupt:
        if batch:
            ch_client.insert('events_db.events', batch, column_names=...)
            print(f"Финальная вставка {len(batch)} событий")
    finally:
        consumer.close()
        ch_client.close()

if __name__ == "__main__":
    from confluent_kafka.error import KafkaError
    main()