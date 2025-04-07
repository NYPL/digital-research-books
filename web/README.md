# Digital Research Books Frontend

[![GitHub version](https://badge.fury.io/gh/NYPL%2Fsfr-bookfinder-front-end.svg)](https://badge.fury.io/gh/NYPL%2Fsfr-bookfinder-front-end)

Digital Research Books' front end application based on NYPL's Reservoir Design System.

Provides a "Welcome page" entry point with heading, search box, and tagline. Connects to an ElasticSearch index via an API endpoint (https://digital-research-books-api.nypl.org).
Simple searches can be entered in the form and an index response is displayed back to the user.

### Requirements

This is a [Next.js](https://nextjs.org/) project bootstrapped with [`create-next-app`](https://github.com/vercel/next.js/tree/canary/packages/create-next-app).

### Contributing: Local Development

#### Getting Started

##### Prerequisites

- Install Node.js v18 or later
- To view pdfs locally through the webreader, you will need to set up a local proxy. If you used environment variables from `.env.sample` you should be able to pull the [web-reader](https://github.com/NYPL-Simplified/web-reader) repo, install it, and run `npm run cors-proxy`. See the web-reader repo for more [instructions](https://github.com/NYPL-Simplified/web-reader#cors-proxy)

1. Install the required packages

```
npm install
```

2. Create a `.env` file and add required environment variables. See `.env.sample` for an example.

#### Running the app locally with npm at `localhost:3000`

```
npm run dev
```

#### Local Hosting

In order to successfully login under a local deployment, you will need to update your machine's `etc/hosts` file. This hosts file maps local host names to ip addresses.

This is necessary because NYPL's authentication system works by reading cookies and parsing the patron's encrypted credentials. These cookies only work on .nypl.org domains, so we need to map our local deployment to a .nypl.org domain.

Add this line to your `etc/hosts` file:

```
	127.0.0.1       local.nypl.org
```

#### Running the app locally with Docker

1. Download and install Docker.
2. `cd` into `sfr-bookfinder-front-end` repo
3. To build and run the application, run:

```
docker-compose up
```

4. Check that app is being served at http://localhost:3000

### Deployment

- We have implemented continuous deployment and integration once changes are merged to main via Github Actions.

### Dependencies

- NextJS
- NYPL Design System
- NYPL Web Reader

### Usage

#### Searchbar

Currently takes in a query string to pass along to the ResearchNow Search API which submits a keyword search to the Elasticsearch instance, and renders the returned output. Sends the `query` parameter specified in the rendered search form on the main page.

Search via keyword, author, title, subject have been implemented. Terms use the AND boolean operator by default. Search terms can also use the OR boolean operator and search terms can be quoted for phrase searching. Combinations of these can be used as well for more complex searching using the basic search input.

Term combinations

- One term: jefferson
- Multiple terms: world war
- Phrase: "English Literature"
- Single term and phrase: james AND "English Literature"

These types of combinations can be used with any available field selection.

#### Filtering

Search Results can be filtered by year range, language and available format.

#### Advanced Search

Advanced Search works like the Simple Search, but allows searching for multiple fields and for pre-filtering. Terms use the AND boolean operator
Term combinations

- Example: Subject:"English Literature" AND Keyword:"Witches"

#### Works and Editions

- Each source record is represented as an Item (something that can actually be read online), which are grouped into Editions (e.g. the 1917 edition of X), which are in turn grouped into Works, (e.g. Moby Dick, or, The Whale). Through this a user can search for and find a single Work record and see all editions of that Work and all of its options for reading online.
- The information and code for this normalization is found in the [drb-etl-pipeline repo](https://github.com/NYPL/drb-etl-pipeline)

#### Accessing books

- Books can be read three ways:
  - Embedded page: DRB embeds a read-online page from a different source. DRB commonly does this for Hathitrust books
  - Webpub reader: DRB serves an epub through the [webpub-viewer](https://github.com/NYPL-Simplified/webpub-viewer/tree/SFR-develop). DRB commonly does this for Gutenberg Project books.
  - Download: DRB offers a link to download the book online. This is often done for PDFs.

### EPUB to Webpub

The EPUB to Webpub Next.js app is deployed at `https://epub-to-webpub.vercel.app`. The **`/api/[containerXmlUrl]`** endpoint is used by the DRB backend to convert `container.xml` files of exploded EPUBs to webpub manifests.

### Test

To run unit tests, run `npm run test` in the terminal.

### C4 Architecture Diagrams

Diagrams for DRB can found via our public [Structurizr link](https://structurizr.com/share/72104) and can be updated in the (c4-diagrams repo)[https://github.com/NYPL-Simplified/c4-diagrams].

### Deprecated

As of September 2023, the [rspec-integration-tests.yml](https://github.com/NYPL/sfr-bookfinder-front-end/actions/workflows/rspec-integration-tests.yml) workflow is no longer in use and has been replaced by the [Playwright.yml](https://github.com/NYPL/sfr-bookfinder-front-end/blob/development/.github/workflows/Playwright.yml) workflow. Please contact the DRB team in Digital for more information.
