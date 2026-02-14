FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV CARGO_HOME=/opt/cargo
ENV RUSTUP_HOME=/opt/rustup
ENV PATH=/opt/cargo/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    build-essential \
    ca-certificates \
    clang \
    curl \
    git \
    libpq-dev \
    libssl-dev \
    pkg-config \
    procps \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable \
    && rustup component add rustfmt clippy

WORKDIR /srv/nexus-api-server

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p nexus-api -p nexus-jobs

ENV NEXUS__APP__HOST=0.0.0.0
ENV NEXUS__APP__PORT=3000

EXPOSE 3000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/srv/nexus-api-server/target/release/nexus-api"]
