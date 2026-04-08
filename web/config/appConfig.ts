const appConfig = {
  appTitle: "Digital Research Books Beta | NYPL",
  appName: "Digital Research Books Beta",
  favIconPath: "//d2znry4lg8s0tq.cloudfront.net/images/favicon.ico",
  port: 3001,
  webpackDevServerPort: 3000,
  baseUrl: "",
  api: {
    url: {
      local: "http://localhost:5050",
      development: "https://drb-api-qa.nypl.org",
      qa: "https://drb-api-qa.nypl.org",
      production: "http://drb-api-qa.nypl.org",
    },
    searchPath: "/search",
    recordPath: "/works",
    editionPath: "/editions",
    readPath: "/links",
    languagesPath: "/utils/languages",
    collectionPath: "/collections",
    chatPath: "/chat",
  },
  booksCount: {
    apiUrl: "/utils/counts",
    experimentName: "BooksCount",
  },
  requestDigital: {
    formUrl: "https://api.airtable.com/v0/appFLZEc3LmVZCRxn/Requests",
    experimentName: "RequestDigital",
  },
  feedback: {
    drbFormURL: "https://api.airtable.com/v0/appFLZEc3LmVZCRxn/Feedback",
    vraFormURL: "https://api.airtable.com/v0/app30jN7kefzFZqq3/Feedback",
  },
  displayCitations: {
    experimentName: "DisplayCitations",
  },
  aboutPageWork: {
    development: "/work/5950e6df-9d99-42fe-8924-1116166a2acb",
    qa: "/work/5950e6df-9d99-42fe-8924-1116166a2acb",
    production: "/work/8771d353-b75f-4f30-a424-e3b9516601f0",
  },
};

export default appConfig;
