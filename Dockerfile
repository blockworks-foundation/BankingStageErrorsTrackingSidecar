# syntax = docker/dockerfile:1.2
FROM rust:1.76-slim-bookworm as base
RUN cargo install cargo-chef@0.1.62 --locked
RUN rustup component add rustfmt
RUN apt-get update && apt-get install -y clang cmake ssh ca-certificates libc6 libssl3 libssl-dev openssl
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
COPY --from=build /app/run-service-and-cleanup.sh /usr/local/bin/

CMD /usr/local/bin/run-service-and-cleanup.sh
