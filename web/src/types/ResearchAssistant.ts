import { FacetItem } from "./DataModel";
import { LinkResult } from "./LinkQuery";
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

export interface HistoryItem {
  results: ChatResults;
  itemId?: string;
  showWebReader: boolean;
  pdfData: ApiItemsRead | null;
  linkResults: LinkResult | null;
}

export type ChatResults =
  | { type: "catalog_search"; data: CatalogSearchResults }
  | { type: "item_search"; data: ItemSearchResults[] }
  | null;

export type SearchParams = {
  query: [string, string][];
  filters?: [string, string][];
};

export type PageType = "vra" | "keyword" | "item";

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
