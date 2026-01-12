import json
import time
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from dateutil import tz

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

def main():
    print("Запуск генератора данных... (Ctrl+C для остановки)")
    while True:
        now = datetime.now(tz=tz.tzlocal())
        event = generate_event(now)
        print(json.dumps(event))
        # Пауза: в среднем 1 событие в секунду, но с вариациями
        time.sleep(random.expovariate(1.0))  # экспоненциальное распределение

if __name__ == "__main__":
    main()
    