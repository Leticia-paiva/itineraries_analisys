#!/bin/bash

timeout_seconds=60

start_time=$(date +%s)
while (( $(date +%s) - start_time < timeout_seconds )); do
    docker stats --format '{{.Name}}\t{{.MemPerc}}\t{{.MemUsage}}' --no-stream
    sleep 5
done