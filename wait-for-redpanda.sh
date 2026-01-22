# wait-for-redpanda.sh
#!/bin/sh
until nc -z redpanda 9092; do
  echo "Ожидание Redpanda..."
  sleep 2
done
echo "✅ Redpanda готова!"
exec "$@"