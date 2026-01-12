import json
import time
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from dateutil import tz
from confluent_kafka import Producer
import json
import sys

fake = Faker()

# Настройки
EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
PRODUCTS = [f"prod_{i}" for i in range(1, 101)]
USERS = list(range(10000, 20000))

def generate_event(now: datetime):
    user_id = random.choice(USERS)
    session_id = str(uuid.uuid4())
    product_id = random.choice(PRODUCTS)
    event_type = random.choices(
        EVENT_TYPES,
        weights=[70, 25, 5],  # 70% просмотров, 25% добавлений, 5% покупок
        k=1
    )[0]

    # Имитация временного паттерна: больше активности с 9 до 22
    hour = now.hour
    base_intensity = 1.0
    if 9 <= hour <= 22:
        base_intensity = 3.0
    if now.weekday() == 4 and now.month == 11 and 20 <= now.day <= 30:  # "Чёрная пятница"
        base_intensity = 10.0

    # Аномалия: 1% шанс на всплеск
    if random.random() < 0.01:
        base_intensity *= 100

    # Задержка, имитирующая интенсивность
    # Но для генератора мы просто возвращаем событие — отправка будет отдельно
    value = 0.0
    if event_type == "purchase":
        value = round(random.uniform(10.0, 500.0), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product_id,
        "timestamp": now.isoformat(),
        "session_id": session_id,
        "value": value
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка доставки: {err}')

def main():
    # Конфигурация продюсера
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'data-generator'
    }
    producer = Producer(conf)
    topic = 'events'

    print("Запуск генератора → отправка в Redpanda...")
    
    def signal_handler(sig, frame):
        print('\nОстановка генератора...')
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        now = datetime.now(tz=tz.tzlocal())
        event = generate_event(now)
        producer.produce(
            topic,
            key=str(event["user_id"]),
            value=json.dumps(event, ensure_ascii=False).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)  # обрабатывает колбэки
        time.sleep(random.expovariate(1.0))  # ~1 событие/сек

if __name__ == "__main__":
    main()
    