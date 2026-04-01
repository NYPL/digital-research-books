#!/bin/bash

# Waits for all declared Docker Compose services with health checks to become healthy.
# Behavior:
# - Identifies services with healthchecks defined in default docker compose file
# - Fails immediately if no services have a healthcheck defined in compose config
# - A health-checked service with no active containers (created, restarting, running) causes immediate failure
# - A health-checked service with more than one active container causes immediate failure
# - A health-checked service container reaching "unhealthy" causes immediate failure
# - A health-checked service is resolved healthy when its active container reaches "healthy"
# - "starting", "none", and empty health states are treated as pending (keep waiting)
# - Polls until all health-checked services are resolved or TIMEOUT expires
# Options:
#   -t, --timeout SECONDS       Override the default timeout (default: 180)
#   -p, --poll-interval SECONDS Override the default poll interval (default: 10)
# Usage: ./docker-compose-healthcheck.sh [-t SECONDS] [-p SECONDS]

set -euo pipefail

# Configuration
POLL_INTERVAL=10
TIMEOUT=180

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--timeout)
            if [[ -z "${2:-}" || ! "$2" =~ ^[0-9]+$ ]]; then
                echo "✗ Error: --timeout requires a positive integer argument"
                exit 1
            fi
            TIMEOUT="$2"
            shift 2
            ;;
        -p|--poll-interval)
            if [[ -z "${2:-}" || ! "$2" =~ ^[0-9]+$ ]]; then
                echo "✗ Error: --poll-interval requires a positive integer argument"
                exit 1
            fi
            POLL_INTERVAL="$2"
            shift 2
            ;;
        *)
            echo "✗ Error: unknown argument '$1'"
            echo "Usage: $0 [-t|--timeout SECONDS] [-p|--poll-interval SECONDS]"
            exit 1
            ;;
    esac
done

START=$SECONDS

echo "Waiting for all health-checked services from docker compose file to become healthy..."
echo "Timeout: ${TIMEOUT}s, Poll interval: ${POLL_INTERVAL}s"
echo ""

# Required commands
if ! command -v docker >/dev/null 2>&1; then
    echo "✗ Error: docker not found in PATH"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "✗ Error: jq is required but not installed"
    exit 1
fi

if ! docker compose ps >/dev/null 2>&1; then
    echo "✗ Error: docker compose ps failed. Is docker compose running?"
    exit 1
fi

# Get compose config and declared services
if ! CONFIG_JSON=$(docker compose config --format json 2>&1); then
    echo "✗ Error: failed to read compose config as JSON (requires Docker Compose v2.17+)"
    echo "$CONFIG_JSON"
    exit 1
fi

if [ "$(echo "$CONFIG_JSON" | jq '.services | length')" -eq 0 ]; then
    echo "✗ Error: no services defined in compose file(s)"
    exit 1
fi

# Get declared services with health-checks
if ! SERVICES_WITH_HC=$(echo "$CONFIG_JSON" | jq -r '
    .services | to_entries[] |
    select(.value.healthcheck != null and ((.value.healthcheck.disable // false) | not)) |
    .key' 2>&1); then
    echo "✗ Error: failed to parse healthcheck config from compose JSON"
    echo "$SERVICES_WITH_HC"
    exit 1
fi

if [ -z "$SERVICES_WITH_HC" ]; then
    echo "✗ Error: no services with a healthcheck defined in compose config"
    exit 1
fi

echo "Health-checked services:"
echo "$SERVICES_WITH_HC" | sed 's/^/  - /'
echo ""

# Poll service states
while [ $((SECONDS - START)) -lt $TIMEOUT ]; do
    echo "[$(date +%H:%M:%S)] Checking service health... ($((SECONDS - START))s elapsed)"

    if ! CONTAINERS_JSON=$(docker compose ps --all --format json | jq -s '.'); then
        echo "  ⚠ Warning: docker compose ps failed, retrying..."
        sleep $POLL_INTERVAL
        continue
    fi

    ALL_RESOLVED=true

    # Iterate health-checked services
    while IFS= read -r svc; do
        # Note: states defined in `docker ps` documentation
        active_containers=$(echo "$CONTAINERS_JSON" | jq -c --arg svc "$svc" \
            '[.[] | select(.Service==$svc and (.State=="running" or .State=="created" or .State=="restarting"))]')
        active_count=$(echo "$active_containers" | jq 'length')

        if [ "$active_count" -eq 0 ]; then
            echo "  ✗ $svc: no active containers — failing immediately"
            echo ""
            echo "Final service states:"
            docker compose ps --all
            exit 1
        fi

        if [ "$active_count" -gt 1 ]; then
            echo "  ✗ $svc: $active_count active containers found (expected exactly 1) — failing immediately"
            echo ""
            echo "Final service states:"
            docker compose ps --all
            exit 1
        fi

        svc_resolved=false

        # Iterate containers for this service (only 1 container)
        while IFS= read -r container; do
            NAME=$(echo "$container" | jq -r '.Name')
            HEALTH=$(echo "$container" | jq -r '.Health // ""')

            # Note: health states defined in `docker ps` documentation
            if [ "$HEALTH" = "healthy" ]; then
                echo "  ✓ $svc ($NAME): healthy"
                svc_resolved=true
            elif [ "$HEALTH" = "unhealthy" ]; then
                echo "  ✗ $svc ($NAME): unhealthy — failing immediately"
                echo ""
                echo "Final service states:"
                docker compose ps --all
                exit 1
            else
                # starting, none, or empty — keep waiting
                echo "  ~ $svc ($NAME): ${HEALTH:-starting}"
            fi
        done <<< "$(echo "$active_containers" | jq -c '.[]')"

        if [ "$svc_resolved" = false ]; then
            ALL_RESOLVED=false
        fi

    done <<< "$SERVICES_WITH_HC"

    echo ""

    if [ "$ALL_RESOLVED" = true ]; then
        echo "✓ All health-checked services are healthy"
        exit 0
    fi

    # Wait before next check
    sleep $POLL_INTERVAL
done

# Timeout reached
echo ""
echo "✗ Timeout reached after $((SECONDS - START))s"
echo ""
echo "Final service states:"
docker compose ps --all
exit 1
