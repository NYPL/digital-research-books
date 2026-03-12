import {
  CatalogSearchResults,
  ChatResults,
  ContentSearchResults,
} from "../types/ResearchAssistant";

export const isCatalogResults = (
  results: ChatResults | null
): results is CatalogSearchResults => {
  return !!results && (results as CatalogSearchResults).editions !== undefined;
};
export const isContentSearchResults = (
  results: ChatResults
): results is ContentSearchResults => {
  return (results as ContentSearchResults).snippets !== undefined;
};
