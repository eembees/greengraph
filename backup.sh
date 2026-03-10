#!/bin/bash
# backup.sh — Daily backup for context-graph-db
# Add to crontab: 0 2 * * * /path/to/context-graph/backup.sh

set -euo pipefail

BACKUP_DIR="$(dirname "$0")/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETAIN_DAYS=${RETAIN_DAYS:-7}

mkdir -p "${BACKUP_DIR}"

echo "Starting backup at ${TIMESTAMP}..."

docker compose exec -T context-graph-db \
    pg_dump -U context_graph -Fc context_graph_db \
    > "${BACKUP_DIR}/backup_${TIMESTAMP}.dump"

# Prune old backups
find "${BACKUP_DIR}" -name "backup_*.dump" -mtime "+${RETAIN_DAYS}" -delete

echo "Backup completed: backup_${TIMESTAMP}.dump"
