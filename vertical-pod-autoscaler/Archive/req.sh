#!/bin/bash

url="http://localhost:30036/api/execute?size_from=10000000&size_to=14010000&sleep_from=100&sleep_to=500"
min=3
max=30

# Generate random number of requests in range
requests=$((min + RANDOM % (max - min + 1)))

echo "Sending $requests requests to $url"

while true; do
for ((i=1; i<=requests; i++)); do
    curl -s "$url" -w "Request $i: %{http_code} in %{time_total}s\n"
done
done
