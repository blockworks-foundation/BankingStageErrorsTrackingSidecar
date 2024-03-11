#!/bin/bash

# bring down whole process tree on exit
trap "kill 0" EXIT

# 432000 = 1 epoch
declare -i SLOTS_TO_KEEP=432000*2

# setup endless loop to run cleanupdb and put it in background
while true; do
  # startup delay
  sleep 300;
  # should retain about 1 week of data
  # no "--count-rows"
  RUST_LOG=info /usr/local/bin/cleanupdb --num-slots-to-keep $SLOTS_TO_KEEP;
  # every 5 hours
  sleep 18000;
done &

/usr/local/bin/grpc_banking_transactions_notifications --rpc-url "$RPC_URL" --grpc-address-to-fetch-blocks "$GEYSER_GRPC_ADDRESS" --grpc-x-token "$GEYSER_GRPC_X_TOKEN" --banking-grpc-addresses "$LIST_OF_BANKING_STAGE_GRPCS" -a /usr/local/bin/alts.txt
