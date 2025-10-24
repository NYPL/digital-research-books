# Digital Research Books ETL Pipeline

![ETL_Pipeline_Tests](https://github.com/NYPL/drb-etl-pipeline/workflows/ETL_Pipeline_Tests/badge.svg)

A containerized python application for importing data from multiple source projects and transforming this data into a unified format that can be accessed via an API (which powers [Digital Research Books Beta](http://digital-research-books-beta.nypl.org/)).

## Process Overview

This ETL pipeline transforms data from various sources into a unified "FRBRized" format where:

- `Item`: Something that can be read online (e.g. a specific digital copy)
- `Edition`: A specific published version (e.g. the 1917 edition)
- `Work`: The abstract creative work (e.g. "Moby Dick")

These records are organized into "clusters" - groups of editions that represent the same work. For example, all editions of "Moby Dick" would be clustered together, making it easy for users to find different versions of the same work.

The pipeline:

1. Imports source records into a Dublin Core Data Warehouse (DCDW)
2. Uses OCLC services to embellish the records with additional metadata
3. Groups records into editions and works using machine learning (clustering)
4. Makes the data available through an API

## API Endpoints

The DRB API is available at:

- Production: [https://digital-research-books-api.nypl.org/](https://digital-research-books-api.nypl.org/)
- QA: [https://drb-api-qa.nypl.org/](https://drb-api-qa.nypl.org/)

Both endpoints provide Swagger documentation at `/apidocs/`.

## Quickstart Guide

This guide provides step-by-step instructions to get the DRB ETL pipeline running locally.

### Prerequisites

- Docker Desktop
- AWS access:
   - submit [this ServiceNow request](https://nyplprod.service-now.com/nyplsp?id=sc_cat_item&sys_id=de6c50b21bc3455090088550cd4bcb4d&sysparm_category=f32c87c413262380c82e7e276144b004) to DevOps
   - Account:`nypl-digital-dev` 
   - Sign in via Azure SSO via http://awsconsole.nypl.org/
   - (optional) Excute command in terminal, you will be prompted to authenticate.
   ```
   aws configure 
   ```
- 

### Setup Steps

1. Clone the repository:

   ```bash
   git clone git@github.com:NYPL/drb-etl-pipeline.git
   cd etl-pipeline
   ```

2. Configure secrets:

   - Create `config/local-secrets.yaml` with the following:
     ```yaml
     AWS_SECRET: xxx
     AWS_ACCESS: xxx
     ```

   - Get the values from AWS (System Manager) Parameter Store (same for QA and PRODUCTION):
      - "drb/\<env\>/aws/access-key"
      - "drb/\<env\>/aws/secret-key"



3. Seed Local Dev DB (one-time only):

   ```bash
   # Run the database seeding process
   docker compose -f docker-compose.setup.yml up --abort-on-container-exit
   ```

4. Regular Startup:

   ```bash
   docker compose up
   ```

   This will start:

   - PostgreSQL database
   - Elasticsearch
   - Redis
   - LocalStack (S3 and SQS)
   - API service

5. Verify the setup:

   - API Documentation: http://127.0.0.1:5050/apidocs/
   - Database: Use PGAdmin4 or your preferred PostgreSQL client:
     ```
     Host: localhost
     Port: 5432
     Database: drb_test_db
     Username: postgres
     Password: localpsql
     ```


 
6. Set up local python env:

Create a virtual environment

*Ensure your virtual Python environment's version matches the project's python version (downgrade if newer).* 

```sh
python -m venv venv
```

Activate the virtual environment. You will need to do this for every terminal session.

```sh
source venv/bin/activate
```

Make sure `wheel` is upgraded to avoid installation errors later

```sh
pip install --upgrade wheel
```

Or

```sh
pip3 install --upgrade pip setuptools wheel
```

Install requirements

```sh
pip install -r requirements.txt
```

Or

```sh
pip3 install -r requirements.txt
```

7. Install GPG

The process for working with books downloaded from Google's GRIN interface requires a decryption step via `gpg`.
`gpg` is pre-installed on most linux distributions but must be installed on MacOs.

Ensure that it is available by installing it via `brew` (if on a Mac) or the appropriate tool for your OS





## Available Processes

The main processes available in this pipeline are:

- `LocalDevelopmentSetupProcess`: Initialize development database
- `SeedLocalDataProcess`: Import sample data
- `APIProcess`: Run the DRB API server
- `IngestProcess`: This process imports data from various sources like HathiTrust, NYPL Catalog, Project Gutenberg, and more.
- `RecordFileSaver`: Store any associated content files (PDFs, etc) in our s3 bucket (this is a more supporting step).
- `RecordEmbellisher`: Using any standard numbers (ISBNs, etc) fetch additional metadata from 3rd parties and add it to the record being processed.
- `RecordClusterer`: Using KMeans clustering to create our work/edition/item data structure.
- `LinkFulfiller`: Ensure that the work record has displayable links via WebPub Manifests.
- `RecordPipelineProcess`: The DRB ETL pipeline. A meta-process that calls the following processes: `RecordEmbellisher`, `RecordClusterer`, `RecordFileSaver`, and `LinkFulfiller`.

Source code for each process can be found from "[processes/\_\_init\_\_.py](processes/__init__.py)"


## Running Individual Processes

While Docker handles the main services, you can run individual processes using:

```bash
python main.py -p ProcessName -e local [options]
```

For example:

```bash
python main.py -p IngestProcess -e local -i daily --source hathitrust
python main.py -p RecordPipelineProcess -e local
```

See `python main.py --help` for all available options.

## Formatting
We use ruff as our formatter. Ensure you have installed the dev requirements.  Run `make format` to format the files. 

We also check formatting before committing. From the root directory, run `pre-commit install` to setup the pre-commit hooks. 

## Testing

This project uses [pytest](https://docs.pytest.org/) for testing. You can run tests using make commands or pytest directly:

```bash
# Run all tests using make commands
make unit           # Run unit tests
make integration    # Run integration tests
make functional     # Run functional tests (requires running Docker environment)

# Run tests directly with pytest
python -m pytest                     # Run all tests
python -m pytest path/to/test.py     # Run a specific test file
python -m pytest -k "test_name"      # Run tests matching "test_name"
python -m pytest -v                  # Run tests with verbose output
```

For more options and detailed usage of pytest, see the [pytest documentation](https://docs.pytest.org/en/stable/how-to/usage.html).

## Formatting

To format new changes, run `ruff format` in the /etl-pipeline directory.

## Deployment

This application uses continuous deployment (CD) via Github Actions to AWS ECS. The full CI/CD pipeline runs automatically when code is merged to `main`.

The deployment process:

1. **Deploy to QA**

   - Builds Docker image
   - Pushes to ECR
   - Deploys to QA environment at [https://drb-api-qa.nypl.org/](https://drb-api-qa.nypl.org/)

2. **Run CI Tests**

   - Runs after QA deployment completes
   - Executes full test suite against QA environment

3. **Deploy to Production**
   - Automatically triggers if QA deployment and tests pass
   - Deploys to production environment at [https://digital-research-books-api.nypl.org/](https://digital-research-books-api.nypl.org/)

You can monitor deployments in:

- GitHub Actions: `.github/workflows/full-ci-cd.yaml`
- AWS ECS Console

## Analytics

Analytics projects are in the [analytics](analytics) folder:

- [University Press Backlist Project](analytics/upress_reporting): Generates Counter 5 reports
  ```bash
  # Example commands
  python3 analytics/upress_reporting/runner.py --start 2024-03-01 --end 2024-03-30
  python3 analytics/upress_reporting/runner.py --year 2025 --quarter Q1
  ```

## Link Flags

Boolean flags used in the API:

- `reader`: Book has Read Online function
- `embed`: Uses third party web reader
- `download`: Book is downloadable
- `catalog`: Book is requestable but not readable online
- `nypl_login`: Requestable by NYPL patrons
- `fulfill_limited_access`: Limited Access book for NYPL patrons
