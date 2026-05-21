import { NextRouter } from "next/router";
import appConfig from "~/config/appConfig";
import { LOGIN_LINK_BASE } from "~/src/constants/links";
import { EditionQuery, EditionResult } from "~/src/types/EditionQuery";
import { FulfillResult } from "~/src/types/FulfillQuery";
import { ApiLanguageResponse } from "~/src/types/LanguagesQuery";
import { LinkResult } from "~/src/types/LinkQuery";
import { WorkQuery, WorkResult } from "~/src/types/WorkQuery";
import { toLocationQuery } from "~/src/util/apiConversion";
import { normalizeCombiningHalfMarksDeep } from "~/src/util/TextNormalization";
import { ApiSearchQuery, ApiSearchResult } from "../../types/SearchQuery";
import { log } from "../newrelic/NewRelic";

const apiEnv = process.env["APP_ENV"];
const apiUrl = process.env["API_URL"] || appConfig.api.url[apiEnv];

const {
  searchPath,
  recordPath,
  editionPath,
  readPath,
  languagesPath,
} = appConfig.api;
const searchUrl = apiUrl + searchPath;
const recordUrl = apiUrl + recordPath;
const editionUrl = apiUrl + editionPath;
const readUrl = apiUrl + readPath;
const languagesUrl = apiUrl + languagesPath;

const defaultWorkQuery: WorkQuery = {
  identifier: "",
  showAll: "true",
};

const defaultEditionQuery = {
  editionIdentifier: "",
  showAll: "true",
};

export const proxyUrlConstructor = () => {
  return (
    process.env["NEXT_PUBLIC_PROXY_URL"] || apiUrl + "/utils/proxy?proxy_url="
  );
};

export const searchResultsFetcher = async (apiQuery: ApiSearchQuery) => {
  if (!apiQuery || !apiQuery.query) {
    throw new Error("no query");
  }

  const searchApiQuery = {
    ...apiQuery,
  };
  const url = new URL(searchUrl);
  url.search = new URLSearchParams(toLocationQuery(searchApiQuery)).toString();

  const res = await fetch(url.toString());
  const searchResult: ApiSearchResult = normalizeCombiningHalfMarksDeep(
    await res.json()
  );

  if (!res.ok) {
    const err = new Error(searchResult.data.message);
    log(err, JSON.stringify(searchResult));
  }
  return searchResult;
};

export const workFetcher = async (query: WorkQuery) => {
  const workApiQuery = {
    showAll:
      typeof query.showAll !== "undefined"
        ? query.showAll
        : defaultWorkQuery.showAll,
  };

  const url = new URL(recordUrl + "/" + query.identifier);
  url.search = new URLSearchParams(workApiQuery).toString();
  const res = await fetch(url.toString());
  const workResult: WorkResult = normalizeCombiningHalfMarksDeep(
    await res.json()
  );

  if (!res.ok) {
    const err = new Error(workResult.data.message);
    log(err, JSON.stringify(workResult));
  }

  return workResult;
};

export const editionFetcher = async (query: EditionQuery) => {
  const editionApiQuery = {
    showAll:
      typeof query.showAll !== "undefined"
        ? query.showAll
        : defaultEditionQuery.showAll,
  };

  const url = new URL(editionUrl + "/" + query.editionIdentifier);
  url.search = new URLSearchParams(editionApiQuery).toString();
  const res = await fetch(url.toString());
  const editionResult: EditionResult = normalizeCombiningHalfMarksDeep(
    await res.json()
  );

  if (!res.ok) {
    const err = new Error(editionResult.data.message);
    log(err, JSON.stringify(editionResult));
  }

  return editionResult;
};

export const languagesFetcher = async () => {
  const url = new URL(languagesUrl);

  const res = await fetch(url.toString());
  const languagesResult: ApiLanguageResponse = await res.json();

  if (!res.ok) {
    const err = new Error(`Cannot find list of languages`);
    log(err, JSON.stringify(languagesResult));
  }

  return languagesResult;
};

export const readFetcher = async (linkId: number) => {
  const url = new URL(readUrl + "/" + linkId);
  const res = await fetch(url.toString());
  const linkResult: LinkResult = await res.json();

  if (!res.ok) {
    const err = new Error(linkResult.data.message);
    log(err, JSON.stringify(linkResult));
  }

  return linkResult;
};

export const fulfillFetcher = async (
  fulfillUrl: string,
  nyplIdentityCookie: any,
  router: NextRouter
) => {
  const url = new URL(fulfillUrl);
  const res = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${nyplIdentityCookie.access_token}`,
    },
  });
  if (res.ok) {
    router.push(res.url);
  } else {
    // redirect to the NYPL login page if access token is invalid
    if (res.status === 401) {
      router.push(LOGIN_LINK_BASE + encodeURIComponent(window.location.href));
    }
    if (res.status === 404) {
      const fulfillResult: FulfillResult = await res.json();
      return fulfillResult.data;
    }
  }
  return undefined;
};
