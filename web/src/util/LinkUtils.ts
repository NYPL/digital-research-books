import { ParsedUrlQuery } from "querystring";

const isRefererInternal = (referer: string, host: string) => {
  return referer && referer.includes(host);
};

export const getBackUrl = (referer: string, host: string) => {
  return isRefererInternal(referer, host) ? referer : "/";
};

export const getBackToSearchUrl = (referer: string, host: string) => {
  return isRefererInternal(referer, host) && referer.includes("/search")
    ? referer
    : null;
};

export const extractQueryParam = (
  query: ParsedUrlQuery,
  param: string
): string | undefined => {
  const extracted = query?.[param];

  return typeof extracted === "string" ? extracted : undefined;
};

export const getHostname = () => {
  return typeof window !== 'undefined' && window.location.origin
    ? window.location.origin
    : '';
}