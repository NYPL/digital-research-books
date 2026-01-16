/* eslint-disable camelcase */

import { FacetItem, Query, Sort } from "./DataModel";
import { ApiWork } from "./WorkQuery";

export type ApiSearchQuery = {
  query: string;
  display?: string;
  filter?: string;
  sort?: string;
  size?: number;
  page?: number;
  showAll?: string;
};

export type ApiFilter = {
  field: string;
  value: any;
};

export type Filter = {
  field: string;
  value: string | number;
};

export type SearchQuery = {
  filters?: Filter[];
  page?: number;
  perPage?: number;
  queries: Query[];
  // Shows the query to display, if different than query.
  display?: Query;
  // While the API supports sorting by multiple parameters, the front end only supports one parameter
  sort?: Sort;
  showAll?: boolean;
};

export const SearchQueryDefaults: SearchQuery = {
  filters: [],
  page: 1,
  perPage: 10,
  queries: [],
  sort: { field: "relevance", dir: "DESC" },
  showAll: false,
};

export type ApiSearchResult = {
  status?: number;
  timestamp?: string;
  responseType?: string;
  data?: ApiSearchData;
};

export type ApiSearchData = {
  facets: { formats: FacetItem[]; languages: FacetItem[] };
  message?: string;
  paging: ApiSearchPaging;
  totalWorks?: number;
  works: ApiWork[];
};

export type ApiSearchPaging = {
  currentPage: number;
  firstPage: number;
  lastPage: number;
  nextPage: number;
  previousPage: number;
  recordsPerPage: number;
};
