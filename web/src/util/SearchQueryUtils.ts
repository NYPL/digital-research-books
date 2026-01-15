import { Query } from "../types/DataModel";
import { Filter } from "../types/SearchQuery";

export const getQueryString = (query: any) =>
  query &&
  Object.keys(query)
    .map((key) =>
      [key, query[key]]
        .map((o) => {
          let ret = o;
          if (typeof o === "object") {
            ret = JSON.stringify(o);
          }
          return encodeURIComponent(ret);
        })
        .join("=")
    )
    .join("&");

export const findQueryForField = (queries: Query[], field: string): Query => {
  return queries.find((query: Query) => {
    return query.field === field;
  });
};

export const findFilterForField = (
  filters: Filter[],
  field: string
): Filter => {
  return filters.find((filter: Filter) => filter.field === field);
};

export const findFiltersForField = (
  filters: Filter[],
  field: string
): Filter[] => {
  return filters.filter((filter: Filter) => {
    return filter.field === field;
  });
};

export const findFiltersExceptField = (
  filters: Filter[],
  field: string
): Filter[] => {
  return filters.filter((filter: Filter) => {
    return filter.field !== field;
  });
};
