#!/usr/bin/env python3

import glob
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
from utils.load_env import parameter_values


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
    
    total = len(secrets_files)
    for index, secrets_file in enumerate(secrets_files, start=1):
        print(f"Processing secrets file {index} of {total}...")
        
        # Load SSM arns
        env_vars = dotenv_values(secrets_file)
        arns = [v for v in env_vars.values() if v]  # Non-empty values only
        
        if not arns:
            print(f"  No variables to mask in secrets file {index}")
            continue
        
        try:
            # Fetch secret values from SSM
            arn_to_value = parameter_values(arns)
            
            # Mask each decrypted value.
            # Multi-line secrets must be split and each line masked individually,
            # as ::add-mask:: only supports single-line values. see: https://github.com/actions/toolkit/blob/main/docs/commands.md#register-a-secret
            masked_count = 0
            for value in arn_to_value.values():
                lines = value.splitlines()
                for line in lines:
                    if line:
                        print(f"::add-mask::{line}")
                masked_count += 1
            
            print(f"  Masked {masked_count} secret(s) from secrets file {index}")
            
        except Exception as e:
            print(f"  Error processing secrets file {index}: {e}")
            sys.exit(1)
    
    print("Secret masking complete")


if __name__ == "__main__":
    main()
