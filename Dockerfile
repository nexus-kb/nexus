FROM docker.io/library/rust:bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked -p nexus-api -p nexus-jobs

FROM docker.io/library/debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 nexus \
    && useradd --uid 10001 --gid nexus --create-home --home-dir /home/nexus --shell /usr/sbin/nologin nexus \
    && mkdir -p /srv/nexus \
    && chown nexus:nexus /srv/nexus

WORKDIR /srv/nexus

COPY --from=builder /build/target/release/nexus-api /usr/local/bin/nexus-api
COPY --from=builder /build/target/release/worker /usr/local/bin/worker

ENV NEXUS__APP__HOST=0.0.0.0
ENV NEXUS__APP__PORT=3000

EXPOSE 3000

USER 10001:10001

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/nexus-api"]
