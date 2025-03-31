#!/bin/bash

cd /mnt/c/Users/mihai/eccos/build_scripts

# Store client PIDs
PIDS=()

# Define cleanup function on interrupt
cleanup() {
  echo "Cleaning up..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null
  done
  wait
  echo "All clients terminated."
  exit 0
}

# Trap SIGINT (Ctrl+C) to trigger cleanup
trap cleanup SIGINT

# Launch all clients in background
for i in {1..5}; do
  echo "Starting client-$i..."
  RUST_LOG=info CONFIG_FILE="./client-${i}-config.toml" \
  cargo run --release --manifest-path="../Cargo.toml" --bin client &
  PIDS+=($!)
done

# Wait for all to complete
wait