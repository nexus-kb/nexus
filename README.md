# Nexus KB

A Linux kernel development knowledge base built with Swift, Vapor, Postgres,
and a SolidJS web interface.

## Backend

```bash
swift build
swift run
swift test
```

The Vapor application requires the existing Postgres environment variables.
In production it serves only the API; nginx serves the web interface and
proxies `/api` requests to Vapor.

## Web interface

Install the frontend dependencies once:

```bash
cd WebUI
pnpm install
```

For frontend development, run Vapor in one terminal and Vite in another:

```bash
# Terminal 1, from the repository root
swift run

# Terminal 2
cd WebUI
pnpm dev
```

Vite proxies `/api` requests to Vapor at `http://127.0.0.1:8080`.

Generate the production assets:

```bash
cd WebUI
pnpm build
```

The build writes untracked production assets to `WebUI/dist/`. The production
nginx image builds these assets directly from the WebUI source.

## Tests

```bash
swift test

cd WebUI
pnpm test
```

The frontend can also be type-checked independently with `pnpm check`.

## VM deployment

The production stack contains ParadeDB/PostgreSQL 18, an internal Vapor API
server, a separate Vapor Queues worker, and the host-facing nginx WebUI.
Database files and lore mirrors remain on the host under `/opt/nexus/db` and
`/opt/nexus/lore`. Postgres is available to VM-local tools and tests on
`127.0.0.1:${NEXUS_DATABASE_PORT:-5432}` but is not exposed publicly.
The database container is limited to 8 GiB of memory and PostgreSQL is sized
with a 2 GiB shared buffer pool plus headroom for queries and parallel workers.

On an Ubuntu 24.04 VM with Docker, grokmirror, `curl`, and `openssl` installed,
clone the repository and run:

```bash
./deploy/setup-vm.sh
```

The idempotent setup creates a private `.env` with a random database password,
installs the tracked grokmirror configuration, installs a cron entry that pulls
the BPF, DAMON, Git, KVM, Linux MM, LKML, LLVM, Netdev, Rust for Linux,
Sched-ext, and Linux Stable archives at minute 17 every four hours, applies
pending SQL migrations, builds the WebUI and Vapor image, starts the stack, and
queues an initial mirror pull followed by maintenance. Edit `.env` before
rerunning setup if the bind address, port, logging, or credentials need to
differ.

Migrations are deliberately manual. On later deployments, run them before
restarting application processes:

```bash
docker compose up -d db
docker compose run --rm migrate
docker compose up -d --build server worker web
```

Applied migration names and checksums are recorded in
`nexus_schema_migrations`; an already-applied SQL file must never be edited.
Add a new numbered migration instead.

Useful operational commands:

```bash
docker compose ps
docker compose logs -f web server worker
sudo /usr/local/sbin/nexus-grokmirror  # pull and queue maintenance now
```
