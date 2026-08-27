for number in {1..100}; do
  curl -s -X POST \
    http://localhost:8888/api/v1/orders \
    -H 'Content-Type: application/json' \
    --data-binary @orders.json > /dev/null
done