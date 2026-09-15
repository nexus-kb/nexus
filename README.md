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
It serves the production web interface from `Public/` at
`http://127.0.0.1:8080/`.

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

Generate the production assets served by Vapor:

```bash
cd WebUI
pnpm build
```

The build replaces `Public/index.html` and `Public/assets/`. These generated
files are committed so a checkout can serve the web interface without running
the frontend toolchain.

## Tests

```bash
swift test

cd WebUI
pnpm test
```

The frontend can also be type-checked independently with `pnpm check`.

## VM deployment

The production stack contains ParadeDB/PostgreSQL 18, the Vapor HTTP server,
and a separate Vapor Queues worker. Database files and lore mirrors remain on
the host under `/opt/nexus/db` and `/opt/nexus/lore`.

On an Ubuntu 24.04 VM with Docker, grokmirror, `curl`, and `openssl` installed,
clone the repository and run:

```bash
./deploy/setup-vm.sh
```

The idempotent setup creates a private `.env` with a random database password,
installs the tracked grokmirror configuration, installs a cron entry that pulls
at minute 17 every four hours, applies pending SQL migrations, builds the WebUI
and Vapor image, and starts the stack. Edit `.env` before rerunning setup if the
bind address, port, logging, or credentials need to differ.

Migrations are deliberately manual. On later deployments, run them before
restarting application processes:

```bash
docker compose up -d db
docker compose run --rm migrate
docker compose up -d --build server worker
```

Applied migration names and checksums are recorded in
`nexus_schema_migrations`; an already-applied SQL file must never be edited.
Add a new numbered migration instead.

Useful operational commands:

```bash
docker compose ps
docker compose logs -f server worker
sudo /usr/local/sbin/nexus-grokmirror  # pull and queue maintenance now
```
