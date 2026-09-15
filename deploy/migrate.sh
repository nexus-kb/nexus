#!/usr/bin/env bash
set -euo pipefail

migrations_directory=/migrations

psql -X --set ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS nexus_schema_migrations (
    name text PRIMARY KEY,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

shopt -s nullglob
migrations=("$migrations_directory"/*.sql)

if ((${#migrations[@]} == 0)); then
    echo "No migrations found in $migrations_directory" >&2
    exit 1
fi

for path in "${migrations[@]}"; do
    name=${path##*/}
    checksum=$(sha256sum "$path" | cut -d ' ' -f 1)
    applied_checksum=$(
        echo "SELECT checksum FROM nexus_schema_migrations WHERE name = :'migration_name';" | \
        psql -X --tuples-only --no-align \
            --set ON_ERROR_STOP=1 \
            --set migration_name="$name"
    )

    if [[ -n "$applied_checksum" ]]; then
        if [[ "$applied_checksum" != "$checksum" ]]; then
            echo "Applied migration $name has changed" >&2
            exit 1
        fi
        echo "Already applied: $name"
        continue
    fi

    echo "Applying: $name"
    {
        cat "$path"
        echo "INSERT INTO nexus_schema_migrations (name, checksum) VALUES (:'migration_name', :'migration_checksum');"
    } | psql -X \
        --set ON_ERROR_STOP=1 \
        --set migration_name="$name" \
        --set migration_checksum="$checksum"
done
