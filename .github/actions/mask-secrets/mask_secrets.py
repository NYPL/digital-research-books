#!/usr/bin/env python3

import glob
import os
import sys
from pathlib import Path

"""
This script loads secrets in .env.*.secrets from AWS Parameter Store and outputs 
GitHub Actions mask commands to prevent the secret values from appearing in workflow logs.

Must be run after AWS credentials are configured.
"""

# Get workspace root (3 levels up from this file) and add etl-pipeline to Python path
workspace_root = Path(__file__).parent.parent.parent.parent
etl_pipeline_dir = workspace_root / "etl-pipeline"
sys.path.insert(0, str(etl_pipeline_dir))

from dotenv import dotenv_values
from utils.load_env import load_secrets


def main():
    """Mask all secrets from .env.*.secrets files."""
    
    # Find all .env.*.secrets files in config directory
    config_dir = etl_pipeline_dir / "config"
    secrets_pattern = str(config_dir / ".env.*.secrets")
    secrets_files = glob.glob(secrets_pattern)
    
    if not secrets_files:
        print("No .env.*.secrets files found to mask")
        return
    
    print(f"Found {len(secrets_files)} secrets file(s) to process")
    
    for secrets_file in secrets_files:
        filename = Path(secrets_file).name
        print(f"Processing {filename}...")
        
        # Read env var names from the file (without fetching from AWS yet)
        env_vars = dotenv_values(secrets_file)
        var_names = [k for k, v in env_vars.items() if v]  # Non-empty values only
        
        if not var_names:
            print(f"  No variables to mask in {filename}")
            continue
        
        try:
            # Use load_secrets() to fetch from SSM and populate os.environ
            load_secrets(secrets_file, raise_if_no_file=True)
            
            # Mask each decrypted value
            masked_count = 0
            for var_name in var_names:
                value = os.environ.get(var_name)
                if value:
                    print(f"::add-mask::{value}")
                    masked_count += 1
            
            print(f"  Masked {masked_count} secret(s) from {filename}")
            
        except Exception as e:
            print(f"  Error processing {filename}: {e}")
            sys.exit(1)
    
    print("Secret masking complete")


if __name__ == "__main__":
    main()
