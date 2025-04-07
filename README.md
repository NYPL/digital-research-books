# Digital Research Books (DRB)

Application for loading records from external sources into the Digital Research Books collection and providing access via API and frontend.

## Overview

This monorepo contains both the frontend and backend components of the Digital Research Books (DRB) application:

- **Frontend** (`/web`): A Next.js application that provides the user interface for searching and accessing digital research books. Features include search functionality, filtering, and multiple ways to access books (embedded reader, webpub reader, and downloads).

- **Backend** (`/etl-pipeline`): A containerized Python application that handles the ETL (Extract, Transform, Load) pipeline for importing data from multiple sources and transforming it into a unified format accessible via API.

## Documentation

For detailed information about each component, please refer to their respective README files:

- [Frontend Documentation](web/README.md)
- [Backend Documentation](etl-pipeline/README.md)

## Quick Links

- Frontend Application: [https://digital-research-books-beta.nypl.org/](https://digital-research-books-beta.nypl.org/)
- QA Application: [https://drb-qa.nypl.org/](https://drb-qa.nypl.org/)
- API Documentation: [https://digital-research-books-api.nypl.org/apidocs/](https://digital-research-books-api.nypl.org/apidocs/)
- QA API: [https://drb-api-qa.nypl.org](https://drb-api-qa.nypl.org)
