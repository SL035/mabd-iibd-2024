#!/bin/sh
until nc -z redpanda 9092 && nc -z clickhouse 8123; do
  echo "⏳ Ожидание redpanda:9092 и clickhouse:8123..."
  sleep 2
done
echo "✅ Все сервисы готовы!"
exec "$@"