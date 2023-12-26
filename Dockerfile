# syntax = docker/dockerfile:1.2
FROM rust:1.70.0 as base
RUN cargo install cargo-chef --locked
RUN rustup component add rustfmt
RUN apt-get update && apt-get install -y clang cmake ssh
WORKDIR /app

FROM base AS plan
COPY . .
WORKDIR /app
RUN cargo chef prepare --recipe-path recipe.json

FROM base as build
COPY --from=plan /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin grpc_banking_transactions_notifications --bin cleanupdb

FROM debian:bullseye-slim as run
RUN apt-get update && apt-get -y install ca-certificates libc6
COPY --from=build /app/target/release/grpc_banking_transactions_notifications /usr/local/bin/
COPY --from=build /app/target/release/cleanupdb /usr/local/bin/
COPY --from=build /app/alts.txt /usr/local/bin/

CMD grpc_banking_transactions_notifications --rpc-url "$RPC_URL" --grpc-address-to-fetch-blocks "$GEYSER_GRPC_ADDRESS" --grpc-x-token "$GEYSER_GRPC_X_TOKEN" --banking-grpc-addresses "$LIST_OF_BANKING_STAGE_GRPCS" -a /usr/local/bin/alts.txt
