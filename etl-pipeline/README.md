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
- AWS access to the `nypl-digital-dev` account (submit ServiceNow request to DevOps)
- Access to required AWS parameter store secrets - ask a team member

### Setup Steps

1. Clone the repository:

   ```bash
   git clone git@github.com:NYPL/drb-etl-pipeline.git
   cd drb-etl-pipeline
   ```

2. Configure secrets:

   - Create `config/local-secrets.yaml` with the following (get values from team member):
     ```yaml
     AWS_SECRET: xxx
     AWS_ACCESS: xxx
     ```

3. Initial Setup (one-time only):

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
   - RabbitMQ
   - Redis
   - LocalStack (S3)
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

Install requirements

```sh
pip install -r requirements.txt
```

You're now ready to run individual processes documented in the following section.

### Running Individual Processes

While Docker handles the main services, you can run individual processes using:

```bash
python main.py -p ProcessName -e local [options]
```

For example:

```bash
# Import HathiTrust records
python main.py -p HathiTrustProcess -e local -i daily

# Run the clustering process
python main.py -p ClusterProcess -e local
```

See `python main.py --help` for all available options.

## Available Processes

The main processes available in this pipeline are:

Core Setup:

- `LocalDevelopmentSetupProcess`: Initialize development database
- `SeedLocalDataProcess`: Import sample data
- `APIProcess`: Run the DRB API

Data Ingestion:
All data ingestion processes can be found in the [`processes/ingest`](processes/ingest) directory. These processes import data from various sources like HathiTrust, NYPL Catalog, Project Gutenberg, and more. Each process follows a similar pattern but is tailored to its specific data source.

Processing:

- `CatalogProcess`: Retrieve OCLC Catalog records
- `ClassifyProcess`: FRBRize records
- `ClusterProcess`: Group records into editions
- `S3Process`: Fetch associated files (ePubs, covers)
- `CoverProcess`: Fetch edition covers

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
