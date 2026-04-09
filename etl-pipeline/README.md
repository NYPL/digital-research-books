# Digital Research Books

<!-- ![ETL_Pipeline_Tests](https://github.com/NYPL/drb-etl-pipeline/workflows/ETL_Pipeline_Tests/badge.svg) -->

This directory contains a containerized python application for importing data from multiple source projects and transforming this data into a unified format that can be accessed via an API. The curated data and API powers the [Virtual Research Assistant](http://digital-research-books-beta.nypl.org/research-assistant) and the legacy [Digital Research Books Beta](http://digital-research-books-beta.nypl.org/).

## ETL Pipeline

The ETL pipeline transforms data from various sources into a unified "FRBRized" format where:

- `Item`: Something that can be read online (e.g. a specific digital copy)
- `Edition`: A specific published version (e.g. the 1917 edition)
- `Work`: The abstract creative work (e.g. "Moby Dick")

These records are organized into "clusters" - groups of editions that represent the same work. For example, all editions of "Moby Dick" would be clustered together, making it easy for users to find different versions of the same work.

The pipeline:

1. Imports source records into a Dublin Core Data Warehouse (DCDW)
2. Uses OCLC services to embellish the records with additional metadata
3. Groups records into editions and works using machine learning (clustering)
4. Makes the data available through an API

## API Server

The API is available at:

- Production: [https://digital-research-books-api.nypl.org/](https://digital-research-books-api.nypl.org/)
- QA: [https://drb-api-qa.nypl.org/](https://drb-api-qa.nypl.org/) (Note: only available on private NYPL sub-net)

Both hosts provide Swagger documentation at `/apidocs/` for DRB-related public endpoints.

## Quickstart Guide

This guide provides step-by-step instructions to set up local development and start the DRB API server running locally in a docker container.

### Prerequisites

- Docker Desktop
   - (optional) sign in with nypl.org email
- AWS access:
   - Submit [a DevOps JIRA ticket](https://newyorkpubliclibrary.atlassian.net/jira/software/c/projects/DOPS/boards/14/backlog) to get your Azure SSO connected to AWS.
      - [Sample ticket](https://newyorkpubliclibrary.atlassian.net/browse/DOPS-1756)
   - Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
   - Sign into AWS console at http://awsconsole.nypl.org/.
   - Choose account:`nypl-digital-dev`
   - Configure the local AWS credentials for CLI and SDK authentication during local dev. Run `aws configure sso`. Follow the steps in the tutorial here: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html#cli-configure-sso-configure .
      - When asked, set profile name to "default". This allows AWS SDKs and CLI to authenticate to this profile without any extra arguments.
      - This authorization is temporary, to re-authenticate with SSO, run `aws sso login`.
- Clone the repository:

   ```bash
   git clone git@github.com:NYPL/drb-etl-pipeline.git
   cd etl-pipeline
   ```


### Run API Server Locally

#### (Option A) Run in docker compose


1. Seed data in the local dockerized DB instance (one-time only):
2. Seed data in the local dockerized DB instance from HathiTrust (one-time only):

   ```bash
   # Run the database seeding process
   docker compose -f docker-compose.setup.yml up --abort-on-container-exit
   ```

   Or to instead seed using a local fixture file:

   ```bash
   # Run dockerized local development setup
   docker compose run --rm --entrypoint python devsetup main.py \
      -p LocalDevelopmentSetupProcess \
      -e docker-compose

   # Run seeding script
   docker compose run --rm --entrypoint python devsetup \
      -m tests.integration.api.assistant.support.seed_frbr_data
   ```

   Note: if the Dockerfile or requirements.txt changed since you last ran docker compose you must add the `--build` option to rebuild the application docker image.

2. Startup Docker Services:

   ```bash
   docker compose up
   ```

   This will start:

   - PostgreSQL database
   - Elasticsearch
   - Redis
   - LocalStack (S3 and SQS)
   - API service

   Note: if the Dockerfile or requirements.txt changed since you last ran docker compose you must add the `--build` option to rebuild the application docker image.

#### (Option B) Run directly on local machine

1. Set up local python env:

   **Create a virtual environment**

   *Ensure your virtual Python environment's version matches the project's python version (downgrade if newer).*

   ```sh
   python -m venv venv
   ```

   **Activate the virtual environment.**
   The following steps assume the virtual environment is active.
   You will need to do this for every terminal session.

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

   **Install requirements**

   ```sh
   pip install -r requirements.txt
   pip install -r dev-requirements.txt
   ```

   **Install pre-commit hooks**
   `pre-commit install`


2. Install GPG

   The process for working with books downloaded from Google's GRIN interface requires a decryption step via `gpg`.
   `gpg` is pre-installed on most linux distributions but must be installed on MacOs.

   Ensure that it is available by installing it via `brew install gnupg` (if on a Mac) or the appropriate tool for your OS.

3. Start localhost server

   `STAGE="development" LOG_LEVEL="debug" DRB_API_HOST=localhost python main.py -p APIProcess --env production`
   - to point at different cloud resources, use a different env file, e.g. `--env qa` or `--env local`


#### Verify the setup

   - Check the API server is up. Navigate to http://127.0.0.1:5050/apidocs/ in your browser.
   - (option A only) Check the local, dockerized DB instance is available. Connect to the local DB using PGAdmin4 or your preferred PostgreSQL client, with the following config:
     ```
     Host: localhost
     Port: 5432
     Database: drb_test_db
     Username: postgres
     Password: localpsql
     ```


## Available Processes

Processes are classes that initialize with arguments in `args_parser.py` and execute with a `.run()` method and thus can be invoked via `main.py`.

The main processes available in this pipeline are:


#### API Server
- `APIProcess`: Run the DRB API server
#### Book Ingest
- `IngestProcess`: This process imports data from various sources like HathiTrust, NYPL Catalog, Project Gutenberg, and more.
- `GRINIngestProcess`: (a) loads data from GRIN hosting to internal s3 (b) ingests the metadata into the Records table (like the `IngestProcess` does for other sources.)
- `GRINConversion`: update our DB with the current status of books sent to Google for digitization and sends books ready for ingest to GRIN Ingest SQS queue.
#### ETL
- `RecordPipelineProcess`: The DRB ETL pipeline. A meta-process that calls the below stand alone processes:
   - `RecordClusterer`: Using KMeans clustering to create our work/edition/item data structure. Indexes "Works" into elastic search for keyword search.
   - `LinkFulfiller`: Ensure that the work record has displayable links via WebPub Manifests.
   - `RecordFileSaver`: Store any associated content files (PDFs, etc) in our s3 bucket (this is a more supporting step).
   - `RecordEmbellisher`: Using any standard numbers (ISBNs, etc) fetch additional metadata from 3rd parties and add it to the record being processed.
#### Local Dev Setup
- `LocalDevelopmentSetupProcess`: Initialize development database
- `SeedLocalDataProcess`: Import sample data

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
We use ruff as our formatter. Ensure you have installed the dev requirements.  Run `make format` or `ruff format` to format the python files.



We also check formatting before committing. If you followed to the set up steps to install the pre-commit hooks, formatting of changed files will be performed at each commit.


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
   - Deploys to QA environment at [https://drb-api-qa.nypl.org/](https://drb-api-qa.nypl.org/) (Note: only available on private NYPL sub-net)

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
