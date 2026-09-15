#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

if ((EUID != 0)); then
    exec sudo -- "$repository_root/deploy/setup-vm.sh" "$@"
fi

for command in curl docker flock grok-pull openssl systemctl systemd-run; do
    if ! command -v "$command" >/dev/null; then
        echo "Required command not found: $command" >&2
        exit 1
    fi
done

install -d -m 0755 /opt/nexus /opt/nexus/db /opt/nexus/lore
install -m 0644 "$repository_root/deploy/grokmirror.conf" \
    /opt/nexus/lore/grokmirror.conf
install -m 0755 "$repository_root/deploy/run-grokmirror.sh" \
    /usr/local/sbin/nexus-grokmirror

if [[ ! -f "$repository_root/.env" ]]; then
    umask 077
    cat >"$repository_root/.env" <<EOF
POSTGRES_USER=nexus
POSTGRES_PASSWORD=$(openssl rand -hex 32)
POSTGRES_DATABASE=nexus
NEXUS_DATABASE_PORT=5432
LOG_LEVEL=info
NEXUS_BIND_ADDRESS=0.0.0.0
NEXUS_PORT=8080
NEXUS_IMAGE_TAG=latest
EOF
    chown "${SUDO_USER:-root}" "$repository_root/.env"
    echo "Created $repository_root/.env"
fi

cd "$repository_root"

docker compose config --quiet

nexus_port=$(sed -n 's/^NEXUS_PORT=//p' .env)
nexus_port=${nexus_port:-8080}
if [[ ! "$nexus_port" =~ ^[0-9]+$ ]] || ((nexus_port < 1 || nexus_port > 65535)); then
    echo "NEXUS_PORT must be an integer between 1 and 65535" >&2
    exit 1
fi

cat >/etc/default/nexus <<EOF
NEXUS_WEBHOOK_URL=http://127.0.0.1:$nexus_port/api/v1/admin/webhooks/grokmirror
EOF
chmod 0644 /etc/default/nexus

cat >/etc/cron.d/nexus-grokmirror <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

17 */4 * * * root /usr/bin/flock -n /run/lock/nexus-grokmirror.lock /usr/local/sbin/nexus-grokmirror
CRON
chmod 0644 /etc/cron.d/nexus-grokmirror
systemctl enable --now cron

docker compose up --detach db
docker compose run --rm migrate
docker compose up --detach --build server worker web

echo "Waiting for Nexus to become healthy..."
for _ in {1..60}; do
    web_container=$(docker compose ps --quiet web)
    health=$(
        docker inspect \
            --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
            "$web_container" 2>/dev/null || true
    )
    if [[ "$health" == healthy ]]; then
        docker compose ps
        systemd-run \
            --collect \
            --no-block \
            --description="Initial Nexus grokmirror pull and maintenance" \
            /usr/bin/flock -n /run/lock/nexus-grokmirror.lock \
            /usr/local/sbin/nexus-grokmirror
        echo "Queued the initial grokmirror pull and maintenance workflow."
        echo "Nexus is available on port $nexus_port."
        exit 0
    fi
    if [[ "$health" == unhealthy || "$health" == exited ]]; then
        docker compose logs --tail 100 web server worker db >&2
        exit 1
    fi
    sleep 2
done

docker compose logs --tail 100 web server worker db >&2
echo "Timed out waiting for Nexus to become healthy" >&2
exit 1
