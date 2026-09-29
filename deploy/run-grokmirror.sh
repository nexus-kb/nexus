#!/usr/bin/env bash
set -euo pipefail
umask 022

if [[ -f /etc/default/nexus ]]; then
    # shellcheck source=/dev/null
    source /etc/default/nexus
fi

if [[ ! ${NEXUS_ADMIN_TOKEN:-} =~ ^[0-9a-f]{64}$ ]]; then
    echo "NEXUS_ADMIN_TOKEN must be 64 lowercase hexadecimal characters" >&2
    exit 1
fi

status=0
webhook_url=${NEXUS_WEBHOOK_URL:-http://127.0.0.1:8080/api/v1/admin/webhooks/grokmirror}

# Mail and mainline updates are independent: a failed mirror must not prevent
# the other source from advancing. The outer cron flock serializes host fetches.
if /usr/bin/grok-pull -c /opt/nexus/lore/grokmirror.conf; then
    /usr/bin/curl --fail --silent --show-error --request POST --header @- \
        "$webhook_url" <<<"Authorization: Bearer $NEXUS_ADMIN_TOKEN" >/dev/null || status=1
else
    status=1
fi

mainline=/opt/nexus/mainline.git
if [[ ! -f "$mainline/HEAD" ]]; then
    /usr/bin/git clone --bare --origin origin \
        https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git \
        "$mainline" || status=1
fi

# An explicit non-forced refspec rejects unexpected mainline rewrites. Fetch
# tags on every tick, including ticks with no new branch commits or lore mail.
if /usr/bin/git --git-dir="$mainline" fetch --atomic --tags origin \
    refs/heads/master:refs/heads/master; then
    /usr/bin/curl --fail --silent --show-error --request POST --header @- \
        "${NEXUS_MAINLINE_URL:-${webhook_url%/webhooks/grokmirror}/mainline/sync}" \
        <<<"Authorization: Bearer $NEXUS_ADMIN_TOKEN" >/dev/null || status=1
else
    status=1
fi

exit "$status"
