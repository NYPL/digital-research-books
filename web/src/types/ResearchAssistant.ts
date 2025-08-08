import { FacetItem } from "./DataModel";
import { ApiSearchPaging } from "./SearchQuery";
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

export interface CatalogSearchResults {
  facets: { formats: FacetItem[]; languages: FacetItem[] };
  totalWorks?: number;
  works: ApiWork[];
  paging: ApiSearchPaging;
  searchParams: SearchParams;
}

export interface ItemSearchResults {
  highlightedText: string[];
  readLink: string;
  textPreview: string;
}

export type ChatResults =
  | { type: "catalog_search"; data: CatalogSearchResults }
  | { type: "item_search"; data: ItemSearchResults[] }
  | null;

export type SearchParams = {
  query: [string, string][];
  filters?: [string, string][];
};

export interface UseResearchAssistantResult {
  messages: Message[];
  sendMessage: (text: string) => Promise<void>;
  itemId: string;
  setItemId: (itemId: string) => void;
  results: ChatResults;
  setResults: (results: ChatResults) => void;
  isLoading: boolean;
  error: string | null;
  clearHistory: () => void;
}

export type ItemReadResults = {
  data: ApiItemsRead;
  status?: number;
  timestamp?: string;
  responseType?: string;
};

export type ApiItemsRead = {
  pageContentType: string;
  pageData: string;
  pageName: string;
  previousPages: string[];
  nextPages: string[];
};
