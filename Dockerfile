# syntax=docker/dockerfile:1.5
FROM lukemathwalker/cargo-chef:latest-rust-1.91.0-slim-trixie AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libssl-dev pkg-config curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

FROM base as plan
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base as build
ARG GITHUB_SHA
ENV GITHUB_SHA ${GITHUB_SHA}
ENV PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig
COPY --from=plan /app/recipe.json .
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bins

FROM debian:trixie-slim as run
RUN apt-get update && apt-get -y install ca-certificates libc6 iputils-ping curl jq

COPY --from=build /app/target/release/url_finder /usr/local/bin/

RUN adduser --system --group --no-create-home finderuser
USER finderuser

CMD ["url_finder"]


