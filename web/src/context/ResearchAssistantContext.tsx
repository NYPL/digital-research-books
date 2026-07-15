import { useRouter } from "next/router";
import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import {
  ChatResultsMap,
  ConversationType,
  HistoryItem,
  Item,
  ItemType,
  MessageRole,
  PageType,
} from "~/src/types/ResearchAssistant";
import {
  isBlinkClient,
  normalizeCombiningHalfMarksDeep,
} from "~/src/util/TextNormalization";
import {
  SURVEY_DELAY_MS,
  SURVEY_SESSION_STORAGE_KEY,
} from "../constants/researchAssistant";
import { FeedbackContext } from "./FeedbackContext";

interface ResearchAssistantViewState {
  editionId?: number;
  itemId: string;
  pageId: string;
  results: ChatResultsMap;
  resultType?: ConversationType;
}

interface ResearchAssistantContextType extends ResearchAssistantViewState {
  messages: Item[];
  sendMessage: (message: string) => Promise<void>;
  setMessages: React.Dispatch<React.SetStateAction<Item[]>>;
  results: ChatResultsMap;
  isLoading: boolean;
  error: string | null;
  historyStack: HistoryItem[];
  setHistoryStack: React.Dispatch<React.SetStateAction<HistoryItem[]>>;
  goToPreviousState: (restoredStack?: HistoryItem[]) => void;
  clearHistory: (page: PageType, invalidateSession?: boolean) => void;
  setViewState: React.Dispatch<React.SetStateAction<any | null>>;
  handlePreview: (url: string) => Promise<void>;
  showChat: boolean;
  toggleChat: () => void;
  isSurveyVisible: boolean;
  setIsSurveyVisible: React.Dispatch<React.SetStateAction<boolean>>;
  markSurveyHandled: () => void;
}

interface PushNewStateArgs {
  results: ChatResultsMap;
  itemId?: string;
  pageId?: string;
  resultType?: ConversationType;
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
  const [hasSentFirstQuery, setHasSentFirstQuery] = useState(false);
  const [activeSearchTimeMs, setActiveSearchTimeMs] = useState(0);
  const [isSurveyVisible, setIsSurveyVisible] = useState(false);
  const [hasSurveyBeenHandled, setHasSurveyBeenHandled] = useState(false);

  const [historyStack, setHistoryStack] = useState<HistoryItem[]>([]);
  const [viewState, setViewState] = useState<ResearchAssistantViewState>({
    itemId: "",
    pageId: "",
    results: null,
  });

  const { setSessionId } = useContext(FeedbackContext);

  const router = useRouter();
  const conversationType = router.pathname.startsWith("/item/")
    ? ConversationType.Content
    : ConversationType.Catalog;

  const markSurveyHandled = React.useCallback(() => {
    if (typeof window !== "undefined") {
      window.sessionStorage.setItem(SURVEY_SESSION_STORAGE_KEY, "true");
    }
    setHasSurveyBeenHandled(true);
    setIsSurveyVisible(false);
  }, []);

  const pushNewState = ({
    results,
    itemId = "",
    pageId = "",
    resultType,
  }: PushNewStateArgs) => {
    setHistoryStack((prevStack) => [
      ...prevStack,
      {
        results: results,
        itemId: itemId,
        pageId: pageId,
        resultType: resultType,
      },
    ]);
  };

  const sendMessage = async (text: string) => {
    if (!text.trim()) return;

    if (!hasSentFirstQuery && !hasSurveyBeenHandled) {
      setHasSentFirstQuery(true);
    }

    setError(null);
    setIsLoading(true);

    const newUserMessage: Item = {
      type: ItemType.Message,
      role: MessageRole.User,
      content: text,
    };
    setMessages((prevMessages) => [...prevMessages, newUserMessage]);

    try {
      const response = await fetch("/api/research-assistant", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message: text,
          conversationType,
          editionId: viewState.editionId,
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

      const rawData = await response.json();
      const data = isBlinkClient()
        ? normalizeCombiningHalfMarksDeep(rawData)
        : rawData;
      setSessionId(data.sessionId);

      const resultsWithCallId = (() => {
        if (!data.toolCallId || !data.results) return data.results;

        if (Array.isArray(data.results)) {
          return data.results.map((result) => ({
            ...result,
            call_id: data.toolCallId,
          }));
        }

        if (
          typeof data.results === "object" &&
          Array.isArray((data.results as any).editions)
        ) {
          return {
            ...data.results,
            editions: (data.results as any).editions.map((edition: any) => ({
              ...edition,
              call_id: data.toolCallId,
            })),
          };
        }

        return data.results;
      })();

      const newMessagesLength = messages.length + data.messages.length;
      setMessages((prevMessages) => [...prevMessages, ...data.messages]);
      const newResults = {
        ...viewState.results,
        [newMessagesLength]: resultsWithCallId,
      };
      setViewState((prev) => ({
        ...prev,
        results: newResults,
        resultType: data.resultType,
      }));

      if (data.resultType === ConversationType.Catalog) {
        setHistoryStack([]);
        pushNewState({
          results: newResults,
          itemId: "",
          resultType: data.resultType,
        });
      } else if (
        data.resultType === ConversationType.Content &&
        viewState.itemId
      ) {
        pushNewState({
          results: newResults,
          itemId: viewState.itemId,
          resultType: data.resultType,
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
      itemId: itemId,
      pageId: pageId,
    }));
    pushNewState({
      results: null,
      itemId: itemId,
      pageId: pageId,
      resultType: viewState.resultType,
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
          resultType: prevState.resultType,
        }));
        return stack.length > 1 ? stack.slice(0, -1) : stack;
      }
      setViewState((prev) => ({
        ...prev,
        results: null,
        itemId: "",
        editionId: undefined,
      }));
      return [];
    });
  };

  const clearHistory = (page: PageType, invalidateSession = false) => {
    if (invalidateSession) clearSession();
    setMessages([]);
    setError(null);
    if (page !== "item") {
      setViewState((prev) => ({
        ...prev,
        results: null,
        itemId: "",
        editionId: undefined,
      }));
    }
  };

  const clearSession = async () => {
    try {
      await fetch("/api/research-assistant-session", {
        method: "DELETE",
      });
    } catch (err) {
      console.error("Failed to clear session:", err);
    }
  };

  useEffect(() => {
    clearSession();
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const hasHandledSurvey =
      window.sessionStorage.getItem(SURVEY_SESSION_STORAGE_KEY) === "true";
    if (hasHandledSurvey) {
      setHasSurveyBeenHandled(true);
      setIsSurveyVisible(false);
      return;
    }
    setHasSurveyBeenHandled(false);
  }, []);

  useEffect(() => {
    if (!hasSentFirstQuery || hasSurveyBeenHandled || isSurveyVisible) return;

    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        setActiveSearchTimeMs((prev) => prev + 1000);
      }
    }, 1000);

    return () => window.clearInterval(timer);
  }, [hasSentFirstQuery, hasSurveyBeenHandled, isSurveyVisible]);

  useEffect(() => {
    if (hasSurveyBeenHandled || isSurveyVisible) return;
    if (activeSearchTimeMs >= SURVEY_DELAY_MS) {
      setIsSurveyVisible(true);
    }
  }, [activeSearchTimeMs, hasSurveyBeenHandled, isSurveyVisible]);

  const toggleChat = useCallback(() => setShowChat((prev) => !prev), []);

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
    showChat,
    toggleChat,
    isSurveyVisible,
    setIsSurveyVisible,
    markSurveyHandled,
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
