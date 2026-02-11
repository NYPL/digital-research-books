import { useRouter } from "next/router";
import React, { createContext, useContext, useState } from "react";
import { readFetcher } from "~/src/lib/api/SearchApi";
import { LinkResult } from "~/src/types/LinkQuery";
import {
  ChatResults,
  ConversationType,
  HistoryItem,
  Item,
  ItemType,
  MessageRole,
  PageType,
} from "~/src/types/ResearchAssistant";

interface ResearchAssistantViewState {
  showWebReader: boolean;
  editionId?: number;
  itemId: string;
  pageId: string;
  results: ChatResults | null;
  linkResults: LinkResult | null;
}

interface ResearchAssistantContextType extends ResearchAssistantViewState {
  messages: Item[];
  sendMessage: (message: string) => Promise<void>;
  setMessages: React.Dispatch<React.SetStateAction<Item[]>>;
  results: ChatResults | null;
  isLoading: boolean;
  error: string | null;
  historyStack: HistoryItem[];
  setHistoryStack: React.Dispatch<React.SetStateAction<HistoryItem[]>>;
  goToPreviousState: (restoredStack?: HistoryItem[]) => void;
  clearHistory: (page: PageType) => void;
  setViewState: React.Dispatch<React.SetStateAction<any | null>>;
  handlePreview: (url: string) => Promise<void>;
  handleReadOnline: (linkId: number) => Promise<void>;
  showChat: boolean;
  toggleChat: () => void;
}

interface PushNewStateArgs {
  results: ChatResults | null;
  showWebReader: boolean;
  linkResults: LinkResult | null;
  itemId?: string;
  pageId?: string;
}

const ResearchAssistantContext = createContext<
  ResearchAssistantContextType | undefined
>(undefined);

export const ResearchAssistantProvider: React.FC<{
  children: React.ReactNode;
}> = ({ children }) => {
  const [messages, setMessages] = useState<Item[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showChat, setShowChat] = useState(true);

  const [historyStack, setHistoryStack] = useState<HistoryItem[]>([]);
  const [viewState, setViewState] = useState<ResearchAssistantViewState>({
    showWebReader: false,
    itemId: "",
    pageId: "",
    results: null,
    linkResults: null,
  });

  const router = useRouter();
  const conversationType = router.pathname.startsWith("/item/")
    ? ConversationType.Content
    : ConversationType.Catalog;

  const pushNewState = ({
    results,
    showWebReader,
    linkResults,
    itemId = "",
    pageId = "",
  }: PushNewStateArgs) => {
    setHistoryStack((prevStack) => [
      ...prevStack,
      {
        results: results,
        itemId: itemId,
        pageId: pageId,
        showWebReader: showWebReader,
        linkResults: linkResults,
      },
    ]);
  };

  const sendMessage = async (text: string) => {
    if (!text.trim()) return;

    setError(null);
    setIsLoading(true);

    const newUserMessage: Item = {
      type: ItemType.Message,
      role: MessageRole.User,
      content: text,
    };
    setMessages((prevMessages) => [...prevMessages, newUserMessage]);

    const messagesForBackend: Item[] = [
      ...messages,
      { type: ItemType.Message, role: MessageRole.User, content: text },
    ];

    try {
      const token = localStorage.getItem("authToken");
      const response = await fetch("/api/research-assistant", {
        method: "POST",
        headers: {
          Authorization: `Basic ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          messages: messagesForBackend,
          editionId: viewState.editionId,
          conversationType,
        }),
      });

      if (response.status === 403) {
        throw new Error(
          "Research Assistant is not enabled in this environment."
        );
      }
      if (!response.ok && response.status !== 201) {
        const errorData = await response.json();
        throw new Error(
          errorData.error ||
            "Failed to get response from research assistant backend."
        );
      }

      const data = await response.json();

      setMessages((prevMessages) => [...prevMessages, ...data.messages]);

      setViewState((prev) => ({
        ...prev,
        results: data.results,
        showWebReader: false,
      }));

      if (data.results?.conversation_context === ConversationType.Catalog) {
        setHistoryStack([]);
        pushNewState({
          results: data.results,
          showWebReader: false,
          linkResults: null,
          itemId: "",
        });
      } else if (
        data.results?.conversation_context === ConversationType.Content &&
        viewState.itemId
      ) {
        pushNewState({
          results: data.results,
          showWebReader: false,
          linkResults: null,
          itemId: viewState.itemId,
        });
      }
    } catch (err: any) {
      console.error("Error sending message:", err);
      setError(err.message || "An unknown error occurred.");

      //   setMessages((prevMessages) =>
      //     prevMessages.map((msg) =>
      //       msg.id === newUserMessage.id
      //         ? { ...msg, status: MessageStatus.Error }
      //         : msg
      //     )
      //   );
    } finally {
      setIsLoading(false);
    }
  };

  const handlePreview = async (url: string) => {
    const urlParts = url.split("/");
    const [itemId, pageId] = [urlParts.at(-3), urlParts.at(-1)];

    setViewState((prev) => ({
      ...prev,
      results: viewState.results,
      itemId: itemId,
      pageId: pageId,
      showWebReader: true,
    }));
    pushNewState({
      results: null,
      showWebReader: true,
      linkResults: null,
      itemId: itemId,
      pageId: pageId,
    });
  };

  const handleReadOnline = async (linkId: number) => {
    const linkResult = await readFetcher(linkId);
    setViewState((prev) => ({
      ...prev,
      results: viewState.results,
      linkResults: linkResult,
      showWebReader: true,
    }));
    pushNewState({
      results: null,
      showWebReader: true,
      linkResults: linkResult,
      itemId: "",
    });
  };

  const goToPreviousState = (restoredStack?: HistoryItem[]) => {
    setHistoryStack((prevStack) => {
      const stack = restoredStack ?? prevStack;
      if (stack.length > 0) {
        const prevState = stack.length > 1 ? stack[stack.length - 2] : stack[0];
        setViewState((prev) => ({
          ...prev,
          results: prevState.results,
          itemId: prevState.itemId || "",
          editionId: prevState.editionId,
          showWebReader: prevState.showWebReader,
          linkResults: prevState.linkResults,
        }));
        return stack.length > 1 ? stack.slice(0, -1) : [];
      }
      setViewState((prev) => ({
        ...prev,
        results: null,
        itemId: "",
        editionId: undefined,
        showWebReader: false,
        linkResults: null,
      }));
      return [];
    });
  };

  const clearHistory = (page: PageType) => {
    setMessages([]);
    setError(null);
    if (page !== "item") {
      setViewState((prev) => ({
        ...prev,
        results: null,
        showWebReader: false,
        itemId: "",
        editionId: undefined,
        linkResults: null,
      }));
    }
  };

  const toggleChat = () => setShowChat((prev) => !prev);

  const value: ResearchAssistantContextType = {
    messages,
    sendMessage,
    setMessages,
    isLoading,
    error,
    historyStack,
    setHistoryStack,
    goToPreviousState,
    clearHistory,
    ...viewState,
    setViewState,
    handlePreview,
    handleReadOnline,
    showChat,
    toggleChat,
  };

  return (
    <ResearchAssistantContext.Provider value={value}>
      {children}
    </ResearchAssistantContext.Provider>
  );
};

export const useResearchAssistant = () => {
  const context = useContext(ResearchAssistantContext);
  if (context === undefined) {
    throw new Error(
      "useResearchAssistant must be used within a ResearchAssistantProvider"
    );
  }
  return context;
};
