# Nexus KB API Server

Nexus KB is a read-focused API and worker backend for mailing-list archives and patch-series exploration. It stores canonical message and pipeline state in Postgres, builds search documents in Meilisearch, and serves public read APIs plus token-protected admin controls. Heavy parsing/indexing work runs in background jobs so request paths stay fast.

## Crate Layout

- `crates/nexus-api`: Axum HTTP server for `/api/v1/*` and `/admin/v1/*`.
- `crates/nexus-jobs`: Worker runtime and pipeline stages.
- `crates/nexus-db`: Database access layer, migrations, and models.
- `crates/nexus-core`: Shared config and core types.
- `crates/nexus-cli`: Small operator/developer CLI helpers.

## Deployment (Packaged Image)

Use the packaged container image:

- `ghcr.io/<owner>/nexus-api-server:latest`

Minimal API run example:

```bash
docker run --name nexus-api --rm -p 3000:3000 \
  -e NEXUS__DATABASE__URL=postgresql://nexus:nexus@<postgres-host>:5432/nexus_kb \
  -e NEXUS__MEILI__URL=http://<meili-host>:7700 \
  -e NEXUS__MEILI__MASTER_KEY=<meili-master-key> \
  -e NEXUS__ADMIN__TOKEN=<admin-token> \
  ghcr.io/<owner>/nexus-api-server:latest
```

For ingest/list-sync jobs, also set `NEXUS__MAIL__MIRROR_ROOT` and mount that path read-only into the container.
