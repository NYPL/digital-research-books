"""
Generate a tests/fixtures/search_catalog_results/ fixture from a real /chat
catalog search. Requires the local API server running against an env with
real catalog data (see tests/fixtures/search_catalog_results/README.md).
Same env as used to start localhost server, must be used to call API and execute
this script.

Usage: PYTHONPATH=. uv run python scripts/gen_search_catalog_fixture.py "<query>" [--env qa] [--outdir <dir>]
"""

import argparse
from datetime import date
from pathlib import Path

import requests

from utils.load_env import load_env
from tests.integration.api.utils import get_vra_auth_headers

FIXTURES_DIR = Path("tests/fixtures/search_catalog_results")

parser = argparse.ArgumentParser()
parser.add_argument("query")
parser.add_argument("--env", default="qa")
parser.add_argument("--outdir", type=Path, default=FIXTURES_DIR)
args = parser.parse_args()
query = args.query

filename = f"{query.replace(' ', '-')}-search_catalog-result-{date.today()}.txt"
out_path = args.outdir / filename

# Used for /chat API key, so must be same env as used to start localhost server
load_env(f"config/.env.{args.env}")
headers = {"Content-Type": "application/json", **get_vra_auth_headers()}

response = requests.post(
    "http://localhost:5050/chat",
    headers=headers,
    json={"conversationType": "catalogSearch", "message": query},
)
response.raise_for_status()

messages = response.json()["data"]["messages"]
output = next(m["output"] for m in messages if m.get("type") == "function_call_output")

with open(out_path, "w") as f:
    f.write(output)

print(f"Wrote {len(output)} chars to {out_path}")
