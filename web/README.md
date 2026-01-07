# Digital Research Books Frontend

[![GitHub version](https://badge.fury.io/gh/NYPL%2Fsfr-bookfinder-front-end.svg)](https://badge.fury.io/gh/NYPL%2Fsfr-bookfinder-front-end)

Front end application for Digital Research Books, based on NYPL's Reservoir Design System.

Provides a "Welcome page" entry point with heading, search box, and tagline. Connects to an ElasticSearch index via an API endpoint (https://digital-research-books-api.nypl.org). Simple searches can be entered in the form and an index response is displayed back to the user.

## Table of Contents

- [Technical Overview](#technical-overview)
- [Getting Started](#getting-started)
- [Running the App](#running-the-app)
- [Local Hosting](#local-hosting)
- [Running with Docker](#running-with-docker)
- [Testing](#testing)
- [Using the App](#using-the-app)
- [EPUB to Webpub](#epub-to-webpub)
- [C4 Architecture Diagrams](#c4-architecture-diagrams)
- [Deployment](#deployment)

## Technical Overview
- [Node.js](https://nodejs.org/) (v22)
- [Next.js](https://nextjs.org/) (bootstrapped with [`create-next-app`](https://github.com/vercel/next.js/tree/canary/packages/create-next-app))
- [NYPL Design System](https://github.com/NYPL/nypl-design-system)
- [NYPL Web Reader](https://github.com/NYPL/web-reader)

## Getting Started

### 1. Install Node.js v22

First check if the expected Node version is already installed:
```
node -v
```

If the output is not something like `v22.xx.x`, then the easiest way to install Node.js v22 is with a version manager like **[nvm](https://github.com/nvm-sh/nvm)**.

Check if nvm is installed with `nvm -v`. If not found, install it with:
```
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
```

After verifying a successfull installation, run:
```
nvm install 22
nvm use 22
```

Alternatively, download and install Node.js v22 from the [official site](https://nodejs.org/en/download).

### 2. Install dependencies
All required dependencies must be installed before the app can be started. While in the `web` directory, run:
```
npm install
```

### 3. Create Environment File
Set up an environment file to store configuration variables and secrets:
1. Create a new file named `.env` in the `web` folder.
2. Copy the contents of `.env.sample` into `.env`.
3. Obtain required secrets from the development team and add them to your environment file as intended.

> ⚠️&nbsp;&nbsp;The app will not function properly without the necessary secrets.

> 🔐&nbsp;&nbsp;Some automated testing depends on user credentials for accessing the catalog.

### 4. (Optional) Set up local PDF proxy
To view PDFs locally through the **webreader**, set up a local proxy as described in its [instructions](https://github.com/NYPL/web-reader#cors-proxy).

## Running the App

To start the application locally at [localhost:3000](http://localhost:3000):

For development:
```
npm run dev
```

For production:
```
npm run build
npm start
```

## Local Hosting

To successfully log in under a local deployment, add the following line to your machine's `/etc/hosts` file:

```
127.0.0.1       local.nypl.org
```

This is necessary because NYPL's authentication system works by reading cookies and parsing the patron's encrypted credentials. These cookies only work on .nypl.org domains, so we need to map our local deployment to a .nypl.org domain

## Running with Docker
To run the application with Docker:
1. Download and install Docker.
2. `cd` into the `web` directory.
3. Build and run the application with `docker compose up`.

The app will be served at http://localhost:3000

## Testing
Run tests in a separate terminal from the app using the commands below.

### Unit Tests

```
npm run test
```

### End-to-End Tests
E2E tests for this application are powered by [Playwright](https://playwright.dev/).

#### 1. Verify Playwright installation

Playwright gets installed with the rest of the application's dependencies when `npm install` is run.

To verify Playwright is installed, run:
```
npx playwright --version
```

If no version number is returned, install Playwright manually:
```
npm install playwright
```

> ℹ️&nbsp;&nbsp;Manual installation may be required if only production dependencies are installed (as in [playwright.yml](https://github.com/NYPL/digital-research-books/blob/main/.github/workflows/playwright.yml)).


#### Install Playwright browsers
Download the browser binaries and their required dependencies with:
```
npx playwright install --with-deps
```

#### Run Playwright tests

To run the entire test suite:
```
npm run playwright
```

To run tests in specific file:
```
npm run playwright /path/to/file
```

## Using the App

### Searchbar

Currently takes in a query string to pass along to the ResearchNow Search API which submits a keyword search to the Elasticsearch instance, and renders the returned output. Sends the `query` parameter specified in the rendered search form on the main page.

Search via keyword, author, title, subject have been implemented. Terms use the AND boolean operator by default. Search terms can also use the OR boolean operator and search terms can be quoted for phrase searching. Combinations of these can be used as well for more complex searching using the basic search input.

Term combinations

- One term: jefferson
- Multiple terms: world war
- Phrase: "English Literature"
- Single term and phrase: james AND "English Literature"

These types of combinations can be used with any available field selection.

### Filtering

Search Results can be filtered by year range, language and available format.

### Advanced Search

Advanced Search works like the Simple Search, but allows searching for multiple fields and for pre-filtering. Terms use the AND boolean operator.

> **Example**:
> Subject:"English Literature" AND Keyword:"Witches"

### Works and Editions

- Each source record is represented as an Item (something that can actually be read online), which are grouped into Editions (e.g. the 1917 edition of X), which are in turn grouped into Works, (e.g. Moby Dick, or, The Whale). Through this a user can search for and find a single Work record and see all editions of that Work and all of its options for reading online.
- The information and code for this normalization is found in the [drb-etl-pipeline repo](https://github.com/NYPL/drb-etl-pipeline)

### Accessing books
Books can be read three ways:
  - **Embedded page**: DRB embeds a read-online page from a different source. DRB commonly does this for Hathitrust books
  - **Webpub reader**: DRB serves an epub through the [webpub-viewer](https://github.com/NYPL-Simplified/webpub-viewer/tree/SFR-develop). DRB commonly does this for Gutenberg Project books.
  - **Download**: DRB offers a link to download the book online. This is often done for PDFs.

## EPUB to Webpub

The EPUB to Webpub Next.js app is deployed at `https://epub-to-webpub.vercel.app`. The `/api/[containerXmlUrl]` endpoint is used by the DRB backend to convert `container.xml` files of exploded EPUBs to webpub manifests.

## C4 Architecture Diagrams

See [Structurizr link](https://structurizr.com/share/72104) and update via the [c4-diagrams repo](https://github.com/NYPL-Simplified/c4-diagrams).

## Deployment

Continuous deployment and integration is implemented via GitHub Actions when changes are merged to `main`.
