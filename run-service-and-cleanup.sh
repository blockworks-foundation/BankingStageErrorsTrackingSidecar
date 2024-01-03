#!/bin/bash

# bring down whole process tree on exit
trap "kill 0" EXIT

# setup endless loop to run cleanupdb and put it in background
while true; do
  # startup delay
  sleep 300;
  # should retain about 1 week of data
  RUST_LOG=info /usr/local/bin/cleanupdb --num-slots-to-keep 2000000;
  # every 5 hours
  sleep 18000;
done &

/usr/local/bin/grpc_banking_transactions_notifications --rpc-url "$RPC_URL" --grpc-address-to-fetch-blocks "$GEYSER_GRPC_ADDRESS" --grpc-x-token "$GEYSER_GRPC_X_TOKEN" --banking-grpc-addresses "$LIST_OF_BANKING_STAGE_GRPCS"
