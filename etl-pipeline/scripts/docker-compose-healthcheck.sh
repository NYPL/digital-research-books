#!/bin/bash

# Waits for all Docker Compose services to become healthy
# If a service with no healthcheck is running that counts as healthy
# if no services are running, exits status 1
# if docker is not running, exits status 1
# if all services are not healthy before TIMEOUT, exits status 1
# MUST be executed with etl-pipeline/ as CWD
# Usage: ./docker-compose-healthcheck.sh

set -e

# Configuration
POLL_INTERVAL=10
TIMEOUT=180
ELAPSED=0

echo "Waiting for all services to become healthy..."
echo "Timeout: ${TIMEOUT}s, Poll interval: ${POLL_INTERVAL}s"
echo ""

# Poll service status
while [ $ELAPSED -lt $TIMEOUT ]; do
    echo "[$(date +%H:%M:%S)] Checking service health... (${ELAPSED}s elapsed)"
    
    # Get service states from docker compose
    SERVICES_JSON=$(docker compose ps --format json 2>&1)
    
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
        STATE=$(echo "$service" | jq -r '.State')
        HEALTH=$(echo "$service" | jq -r '.Health')
        
        # Check health status
        # services with healthchecks should be "healthy", 
        # services without healthchecks should be "running"
        if [ "$HEALTH" = "healthy" ]; then
            echo "  ✓ $NAME: $STATE ($HEALTH)"
        elif [ "$HEALTH" = "" ] && [ "$STATE" = "running" ]; then
            echo "  ✓ $NAME: $STATE (no healthcheck)"
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
