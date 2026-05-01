import { SURVEY_SESSION_STORAGE_KEY } from "../constants/researchAssistant";
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
  results: ChatResults | null
): results is ContentSearchResults => {
  return !!results && (results as ContentSearchResults).snippets !== undefined;
};

export const getSurveyStorageKey = (sessionId?: string) => {
  return `${SURVEY_SESSION_STORAGE_KEY}:${sessionId}`;
};
