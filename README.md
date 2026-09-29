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

The idempotent setup creates a private `.env` with a random database password and admin token,
installs the tracked grokmirror configuration, installs a cron entry that pulls
the BPF, DAMON, KVM, Linux MM, LKML, LLVM, Netdev, Rust for Linux,
Sched-ext, and Linux Stable archives at minute 17 every four hours, applies
pending SQL migrations, builds the WebUI and Vapor image, starts the stack, and
queues an initial mirror pull followed by maintenance. Edit `.env` before
rerunning setup if the bind address, port, logging, or credentials need to
differ.

Migration `0021` removes the Git development mailing list and Git-only data,
preserving messages cross-posted to retained lists and placeholder parents needed
by retained replies. For existing installations, back up the database, hold
`/run/lock/nexus-grokmirror.lock`, drain maintenance, and pause maintenance writers
before applying it. Install the updated `deploy/grokmirror.conf` at
`/opt/nexus/lore/grokmirror.conf` before resuming maintenance; update any external
configuration-management copy too. The old `/opt/nexus/lore/git` archive is no
longer synced and may be retained offline for rollback. Do not remove
`/opt/nexus/mainline.git`, which is the Linux mainline mirror.

All `/api/v1/admin/` endpoints require `Authorization: Bearer <token>`, including
localhost requests. Set `NEXUS_ADMIN_TOKEN` to the output of `openssl rand -hex 32`
for native development and existing deployments; server and worker startup fail
without a valid 64-character lowercase hex token. Setup generates it only when
absent and preserves it on subsequent runs. Public APIs and health checks need no token.
Keep `.env` private and store the same token in root-owned mode-0600
`/etc/default/nexus` for the mirror script. Ansible deployments should source both
files from the same encrypted secret, outside the checkout. Never log the token
or put it in URLs or frontend configuration. Docker administrators can read container
environment variables. Use HTTPS for remote requests; HTTP is only for local callers.
Rotate the token while holding `/run/lock/nexus-grokmirror.lock`: update both files
and recreate server and worker before releasing the lock.

For the operator examples below, load `NEXUS_ADMIN_TOKEN` securely into your shell.
The header is passed on stdin so it does not appear in curl's command-line arguments.

Migrations are deliberately manual. On later deployments, run them before
restarting application processes:

```bash
docker compose up -d db
docker compose run --rm migrate
deployment_tag="${NEXUS_IMAGE_TAG:-latest}-deploy-$(date -u +%Y%m%d%H%M%S)-$$"
NEXUS_IMAGE_TAG="$deployment_tag" docker compose build server web
expected_app_image=$(docker image inspect --format '{{.Id}}' "nexus-kb:$deployment_tag")
NEXUS_IMAGE_TAG="$deployment_tag" docker compose up -d --force-recreate server worker web
test "$(docker inspect --format '{{.Image}}' "$(docker compose ps -q server)")" = "$expected_app_image"
test "$(docker inspect --format '{{.Image}}' "$(docker compose ps -q worker)")" = "$expected_app_image"
```

The unique deployment tag prevents another build from changing the selected
image between build and startup. Both application services are then checked
against the exact image produced by this deployment.

Applied migration names and checksums are recorded in
`nexus_schema_migrations`; an already-applied SQL file must never be edited.
Add a new numbered migration instead.

After deploying the revision-link matcher (migration `0019`), rebuild lineage
for existing lists to extract their cover-letter version references. Incremental
maintenance only processes queued patchsets; changing the matcher version does
not automatically backfill old records. For BPF, an operator can queue:

```bash
curl --fail-with-body -X POST \
  --header @- <<<"Authorization: Bearer $NEXUS_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"full"}' \
  http://127.0.0.1:8080/api/v1/admin/mailing-lists/bpf/patch-lineage
```

This changes stored lineage assignments, not archive messages, and preserves
manual locks. Monitor the returned operation ID at
`/api/v1/admin/operations/<id>`. No mirror pull or full message re-ingest is needed.
Revision links currently recognize explicit `vN:` lore URLs on the same or next
line. They require matching authors/phases, an older matching revision, and an
earlier timestamp; ordinary discussion links are not lineage evidence.

## Mainline patch tracking

Linus's full-history bare repository lives at `/opt/nexus/mainline.git`, beside
the mail archives at `/opt/nexus/lore`. The host fetches `master` and tags from
`https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git`; application
containers mount it read-only. The four-hour mirror script independently queues
mainline indexing even when no mail changed. A failed lore pull does not prevent
mainline maintenance, and a failed mainline fetch leaves the previous index intact.

Deploy migration `0020` before the new application, and install the updated host
script (`sudo install -m 0755 deploy/run-grokmirror.sh /usr/local/sbin/nexus-grokmirror`).
For existing installations, clone the bare repository before recreating containers:

```bash
sudo git clone --bare --origin origin \
  https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git \
  /opt/nexus/mainline.git
```

Skip that clone if the repository already exists. `setup-vm.sh` handles this on
new installations. The initial index covers commits **after `v2.6.12`**, the first
final release backed by a commit in this tree. The `v2.6.11` tags reference a tree
snapshot, not commit history. The boundary release itself and its ancestors are
excluded. `MAINLINE_BASE_REF` sets this boundary; moving it to an older
ancestor expands coverage on the next run. Narrowing coverage or a non-fast-forward
mainline rewrite fails visibly rather than silently retaining invalid results.
An interrupted run must resume with the same base commit before changing coverage.
Native development can override `MAINLINE_REPO_PATH`; Compose uses the path above.
The first backfill is heavier than subsequent incremental runs and shares the
existing queue worker. Progress appears in worker logs.

```bash
# Index the current local clone (does not fetch).
curl --fail-with-body --header @- -X POST http://127.0.0.1:8080/api/v1/admin/mainline/sync <<<"Authorization: Bearer $NEXUS_ADMIN_TOKEN"
curl --fail-with-body --header @- http://127.0.0.1:8080/api/v1/admin/mainline <<<"Authorization: Bearer $NEXUS_ADMIN_TOKEN"
# Reverse lookup: full hash or an unambiguous prefix of at least seven characters.
curl --fail-with-body http://127.0.0.1:8080/api/v1/commits/COMMIT_HASH
```

Lineage statuses distinguish unchecked, no match, partial, merged/unreleased, and
merged/released revisions. Versions come from ancestry against **final mainline
release tags only**, never RCs or stable-backport tags. The series version is the
first final release containing every part of a complete revision; each commit also
shows its own first release. Results expose the indexed boundary and last check.
During incomplete or failed indexing, lineage results show unchecked without
provisional commit evidence, and reverse lookup returns HTTP 503 until recovery.

Stable patch IDs identify equivalent changes; a corroborating Message-ID link
additionally identifies a source submission. Identical resends can all match the
same commit without proving which revision was applied. Unrelated links alone do
not establish a match. Rename-aware and delete/add diffs are both indexed.
Whitespace is normalized; edited, squashed, or split patches
may be missed. “No mainline match found” is not proof of non-merge, and historical
inclusion does not imply that a change has never been reverted. Commit lookup only
covers indexed history. Unimported mailing-list references remain available as lore
links, explicitly distinguished from confirmed source matches.

The mainline integration tests reset the singleton index and are guarded by
`NEXUS_TEST_DISPOSABLE=1`. Set this **only with a disposable database**, migrate it,
then run `swift test` with its `POSTGRES_*` connection settings. Tests construct a
small local bare Git history; they do not fetch or modify the mainline clone.
An optional real-clone check can be enabled with `NEXUS_MAINLINE_SMOKE_BASE=v7.1`
and `swift test --filter indexesRealMainline`, using the same disposable database.
It compares indexed commit counts and sampled release assignments with Git and
checks an incremental rerun; it reads the clone without changing it.

Useful operational commands:

```bash
docker compose ps
docker compose logs -f web server worker
sudo /usr/local/sbin/nexus-grokmirror  # pull and queue maintenance now
```
