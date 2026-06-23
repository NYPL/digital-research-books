#!/bin/bash
set -euo pipefail

# Ensure docker services are running
docker compose up --no-recreate --wait

source .venv/bin/activate

# Seed DRB data into DB. `-e local` specifies the local dockerized DB instance as the target.
python main.py -e local -p SeedLocalDataProcess -i daily

# Seed VRA data into DB. `-e local` specifies the local dockerized DB instance as the target.
python -m tests.integration.api.assistant.support.seed_frbr_data -e local

#  ALT: make SeedLocalDataProcess fully idempotent so it won't alter the DB state if it's already run
# then add seeddb to main docker compose file with `depends_on: devsetup; condition: service_completed_successfully`
# ideas to make SeedLocalDataProcess idempotent: just  check if there are the same number of records present \
#  in the db as the seed process fetches (50 records)

# ALT: use a concise docker-compose.seed.yaml that just overrides devsetup cmd to do both init \
# and seed, called when needed by docker compose up -f docker-compose.yaml -f docker-compose.seed.yaml \
# per https://docs.docker.com/reference/cli/docker/compose/#use--f-to-specify-the-name-and-path-of-one-or-more-compose-files 
