import {
  Agent,
  Date,
  ItemLink,
  Language,
  LinkFlags,
  Subject,
} from "./DataModel";
import { LinkResult } from "./LinkQuery";

export type Item = MessageItem | ToolCallItem | ToolCallOutputItem;

export type MessageItem =
  | {
      type?: ItemType.Message;
      role: MessageRole.User | MessageRole.System;
      content: string;
      id?: string;
      status?: MessageStatus;
    }
  | {
      type?: ItemType.Message;
      role: MessageRole.Assistant;
      content: OutputContentItem[];
      id?: string;
      status?: MessageStatus;
    };

export interface OutputContentItem {
  type: "output_text";
  text: string;
  annotations?: unknown[];
  logprobs?: unknown[];
}

interface ToolCallItem {
  type: ItemType.FunctionCall;
  id?: string;
  name: string;
  arguments: string; // JSON-serialized arguments
  call_id: string;
}

interface ToolCallOutputItem {
  type: ItemType.FunctionCallOutput;
  call_id: string;
  output: string; // Serialized JSON
  name?: string;
}

export enum ItemType {
  Message = "message",
  FunctionCall = "function_call",
  FunctionCallOutput = "function_call_output",
}

export enum MessageRole {
  User = "user",
  Assistant = "assistant",
  System = "system",
}

export enum MessageStatus {
  Sending = "sending",
  Sent = "sent",
  Error = "error",
}

export enum ConversationType {
  Catalog = "catalogSearch",
  Content = "contentSearch",
}

export interface CatalogLink {
  id: number;
  media_type: string;
  url: string;
  flags: LinkFlags;
  content?: string;
}

export interface CatalogRight {
  license?: string;
  rights_statement?: string;
  source?: string;
}

export interface CatalogItem {
  content_type: string;
  contributors: Agent[];
  drm?: string;
  edition_id: number;
  id: number;
  links: CatalogLink[];
  measurements?: string;
  physical_location?: string;
  rights: CatalogRight[];
  source: string;
}

export interface Snippet {
  chunk_score: number;
  start_page: number;
  end_page: number;
  item_id: number;
  text: string;
}

export interface CatalogEdition {
  alt_titles: string[];
  contributors: string[];
  dates: Date[];
  edition?: string;
  edition_statement?: string;
  extent?: string;
  id: number;
  items: CatalogItem[];
  languages: Language[];
  links: ItemLink[];
  measurements: string[];
  publication_date?: string;
  publication_place?: string;
  publishers: Agent[];
  snippets: Snippet[];
  sub_title?: string;
  summary?: string;
  table_of_contents?: string;
  title: string;
  volume?: string;
  work_alt_titles: string[];
  work_authors: Agent[];
  work_contributors: Agent[];
  work_dates?: Date[];
  work_id?: number;
  work_languages: Language[];
  work_measurements: string[];
  work_medium?: string;
  work_series?: string;
  work_series_position?: string;
  work_sub_title?: string;
  work_subjects: Subject[];
  work_title?: string;
  work_uuid?: string;
}

export type CatalogSearchResults = {
  editions: CatalogEdition[];
  search_params: SearchParams;
  paging: {
    recordsPerPage: number;
    firstPage: number;
    previousPage: number;
    currentPage: number;
    nextPage: number;
    lastPage: number;
    totalRecords: number;
  };
};

export interface ContentSearchResults {
  snippets: Snippet[];
  search_params: SearchParams;
}

export interface HistoryItem {
  results: ChatResults;
  itemId?: string;
  editionId?: number;
  showWebReader: boolean;
  linkResults: LinkResult;
  resultType?: ConversationType;
}

export type ChatResults = CatalogSearchResults | ContentSearchResults;

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

export type FeedbackState = "up" | "down";
