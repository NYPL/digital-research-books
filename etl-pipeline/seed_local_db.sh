#!/bin/bash
set -euo pipefail

./docker-compose-healthcheck.sh

source .venv/bin/activate
python main.py -e local -p SeedLocalDataProcess -i daily

#  ALT: make SeedLocalDataProcess fully idempotent so it won't alter the DB state if it's already run
# then add seeddb to main docker compose file with `depends_on: devsetup; condition: service_completed_successfully`
# ideas to make SeedLocalDataProcess idempotent: just  check if there are the same number of records present \
#  in the db as the seed process fetches (50 records)

# TODO: Seed from fixture file
