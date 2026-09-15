#!/usr/bin/env bash
set -euo pipefail
umask 022

if [[ -f /etc/default/nexus ]]; then
    # shellcheck source=/dev/null
    source /etc/default/nexus
fi

/usr/bin/grok-pull -c /opt/nexus/lore/grokmirror.conf

# Queue incremental ingestion and patch-lineage maintenance after each pull.
/usr/bin/curl \
    --fail \
    --silent \
    --show-error \
    --request POST \
    "${NEXUS_WEBHOOK_URL:-http://127.0.0.1:8080/api/v1/admin/webhooks/grokmirror}" \
    >/dev/null
