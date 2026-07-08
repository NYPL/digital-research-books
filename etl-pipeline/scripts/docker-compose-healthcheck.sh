#!/bin/bash

# Waits for all Docker Compose services to become healthy
# If a service with no healthcheck is running that counts as healthy
# if no services are running, exits status 1
# if docker is not running, exits status 1
# if all services are not healthy before TIMEOUT, exits status 1
# MUST be executed with etl-pipeline/ as CWD
# Usage: [POLL_INTERVAL=N TIMEOUT=M] ./docker-compose-healthcheck.sh

set -e

# Defaults
POLL_INTERVAL=${POLL_INTERVAL:-10}
TIMEOUT=${TIMEOUT:-180}


echo "Waiting for all services to become healthy..."
echo "Timeout: ${TIMEOUT}s, Poll interval: ${POLL_INTERVAL}s"
echo ""

# Services defined by the compose file(s) being started, e.g. excludes one-off
# setup files like docker-compose.setup.yml that are run manually and separately
EXPECTED_SERVICES=$(docker compose config --services 2>&1)
if [ $? -ne 0 ]; then
    echo "✗ Error: docker compose config failed. Is docker compose running?"
    echo "$EXPECTED_SERVICES"
    exit 1
fi

# Absolute path of the compose file this script resolves against, used to
# verify a container's provenance rather than trusting service names to be
# unique across compose files sharing this directory's project namespace
EXPECTED_CONFIG_FILE=$(realpath docker-compose.yml)

# Poll service status
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    echo "[$(date +%H:%M:%S)] Checking service health... (${ELAPSED}s elapsed)"
    
    # Get service states from docker compose
    SERVICES_JSON=$(docker compose ps --all --format json 2>&1)
    
    # early return if docker status check failed
    if [ $? -ne 0 ]; then
        echo "✗ Error: docker compose ps failed. Is docker compose running?"
        echo "$SERVICES_JSON"
        exit 1
    fi
    
    # early return if no docker compose services running
    if [ -z "$SERVICES_JSON" ]; then
        echo "✗ Error: No services found. Please start docker compose services first."
        exit 1
    fi

    ALL_HEALTHY=true
    
    # Iterate thru each service status json
    while IFS= read -r service; do
        # Parse service states
        NAME=$(echo "$service" | jq -r '.Name')
        SERVICE=$(echo "$service" | jq -r '.Service')
        STATE=$(echo "$service" | jq -r '.State')
        HEALTH=$(echo "$service" | jq -r '.Health')
        EXIT_CODE=$(echo "$service" | jq -r '.ExitCode')
        LABELS=$(echo "$service" | jq -r '.Labels')
        CONFIG_FILES=$(grep -oE 'com\.docker\.compose\.project\.config_files=[^,]*' <<< "$LABELS" | cut -d= -f2-)

        # Skip containers not defined by the compose file(s) being started
        # (e.g. leftover one-off containers from a separately run setup file)
        if ! grep -qxF "$SERVICE" <<< "$EXPECTED_SERVICES"; then
            continue
        fi

        # Skip containers not actually created from this compose file, in case
        # another -f file sharing this directory's project namespace declares
        # a same-named service (name match alone can't tell them apart)
        if [[ ",$CONFIG_FILES," != *",$EXPECTED_CONFIG_FILE,"* ]]; then
            continue
        fi

        # Check health status
        # services with healthchecks should be "healthy",
        # services without healthchecks should be "running"
        # one-shot services (e.g. devsetup/seed) with no healthcheck exit when done,
        # so distinguish a clean exit(0) from a crash by ExitCode rather than STATE
        if [ "$HEALTH" = "healthy" ]; then
            echo "  ✓ $NAME: $STATE ($HEALTH)"
        elif [ "$HEALTH" = "" ] && [ "$STATE" = "running" ]; then
            echo "  ✓ $NAME: $STATE (no healthcheck)"
        elif [ "$HEALTH" = "" ] && [ "$STATE" = "exited" ] && [ "$EXIT_CODE" = "0" ]; then
            echo "  ✓ $NAME: $STATE (exited 0, no healthcheck)"
        else
            echo "  ✗ $NAME: $STATE ($HEALTH)"
            ALL_HEALTHY=false
        fi
    done <<< "$SERVICES_JSON"
    
    echo ""
    
    # Check if all services are healthy
    if [ "$ALL_HEALTHY" = true ]; then
        echo "✓ All services are healthy!"
        exit 0
    fi
    
    # Wait before next check
    sleep $POLL_INTERVAL
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

# Timeout reached
echo ""
echo "✗ Timeout reached after ${TIMEOUT}s. Not all services are healthy."
echo ""
echo "Final service states:"
docker compose ps
exit 1
