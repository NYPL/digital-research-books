import { FacetItem } from "./DataModel";
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
}

export interface UseResearchAssistantResult {
  messages: Message[];
  sendMessage: (text: string) => Promise<void>;
  results: ChatResults;
  isLoading: boolean;
  error: string | null;
  clearHistory: () => void;
}