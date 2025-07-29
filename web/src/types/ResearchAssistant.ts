import { FacetItem, Query } from "./DataModel";
import { ApiSearchPaging, Filter, SearchQuery } from "./SearchQuery";
import { ApiWork } from "./WorkQuery";

export interface Message {
  id: string;
  status?: MessageStatus;
  type: MessageType;
  data: { content: string };
}

export enum MessageType {
  Human = "human",
  Ai = "ai",
  System = "system",
  Tool = "tool",
}

export enum MessageStatus {
  Sending = "sending",
  Sent = "sent",
  Error = "error",
}

export interface ChatResults {
  facets: { formats: FacetItem[]; languages: FacetItem[] };
  totalWorks?: number;
  works: ApiWork[];
  paging: ApiSearchPaging;
  searchParams: SearchParams;
}

export type SearchParams = {
  query: [string, string][];
  filters?: [string, string][];
};

export interface UseResearchAssistantResult {
  messages: Message[];
  sendMessage: (text: string) => Promise<void>;
  results: ChatResults;
  setResults: (results: ChatResults) => void;
  isLoading: boolean;
  error: string | null;
  clearHistory: () => void;
}