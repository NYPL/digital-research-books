import React, { createContext, useContext, useState } from "react";
import {
    ChatResults,
    ApiItemsRead,
    Message,
    MessageStatus,
    MessageType,
    HistoryItem,
} from "~/src/types/ResearchAssistant";
import { LinkResult } from "~/src/types/LinkQuery";
import { itemsReadFetcher } from "~/src/lib/api/ResearchAssistantApi";
import { readFetcher } from "~/src/lib/api/SearchApi";

interface ResearchAssistantViewState {
    showWebReader: boolean;
    pdfData: ApiItemsRead | null;
    itemId: string;
    results: ChatResults | null;
    linkResults: LinkResult | null;
}

interface ResearchAssistantContextType extends ResearchAssistantViewState {
    messages: Message[];
    sendMessage: (message: string) => Promise<void>;
    sendMessageStream: (text: string, onPartial?: (partial: string) => void) => Promise<void>;
    results: ChatResults | null;
    isLoading: boolean;
    error: string | null;
    historyStack: HistoryItem[];
    goToPreviousState: () => void;
    clearHistory: () => void;
    setViewState: React.Dispatch<React.SetStateAction<any | null>>;
    handlePreview: (url: string) => Promise<void>;
    handleReadOnline: (linkId: number) => Promise<void>;
}

interface PushNewStateArgs {
    results: ChatResults | null;
    showWebReader: boolean;
    pdfData: ApiItemsRead | null;
    linkResults: LinkResult | null;
    itemId?: string;
}

const ResearchAssistantContext = createContext<
    ResearchAssistantContextType | undefined
>(undefined);

export const ResearchAssistantProvider: React.FC<{
    children: React.ReactNode;
}> = ({ children }) => {
    const [messages, setMessages] = useState<Message[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const [historyStack, setHistoryStack] = useState<HistoryItem[]>([]);
    const [viewState, setViewState] = useState<ResearchAssistantViewState>({
        showWebReader: false,
        pdfData: null,
        itemId: "",
        results: null,
        linkResults: null,
    });

    const pushNewState = ({
        results,
        showWebReader,
        pdfData,
        linkResults,
        itemId = "",
    }: PushNewStateArgs) => {
        setHistoryStack((prevStack) => [
            ...prevStack,
            {
                results: results,
                itemId: itemId,
                showWebReader: showWebReader,
                pdfData: pdfData,
                linkResults: linkResults,
            },
        ]);
    };

    const sendMessage = async (text: string) => {
        if (!text.trim()) return;

        setError(null);
        setIsLoading(true);

        const newUserMessage: Message = {
            id: Date.now().toString() + "-user",
            data: { content: text },
            status: MessageStatus.Sending,
            type: MessageType.Human,
        };
        setMessages((prevMessages) => [...prevMessages, newUserMessage]);

        let textToSend = text;
        if (viewState.itemId !== "") {
            textToSend += `<ItemId>${viewState.itemId}</ItemId>`;
        }

        const messagesForBackend = [
            ...messages.map((msg) => ({
                type: msg.type,
                data: { content: msg.data.content },
            })),
            { type: MessageType.Human, data: { content: textToSend } },
        ];

        try {
            const token = localStorage.getItem("authToken");
            const response = await fetch("/api/research-assistant", {
                method: "PUT",
                headers: {
                    "Authorization": `Basic ${token}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    messages: messagesForBackend,
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

            const assistantMessage: Message = {
                id: Date.now().toString() + "-assistant",
                data: {
                    content: data.answer || "No response provided.",
                },
                status: MessageStatus.Sent,
                type: MessageType.Ai,
            };

            setMessages((prevMessages) =>
                prevMessages
                    .map((msg) =>
                        msg.id === newUserMessage.id
                            ? { ...msg, status: MessageStatus.Sent }
                            : msg
                    )
                    .concat(assistantMessage)
            );

            setViewState((prev) => ({
                ...prev,
                results: data.results,
                showWebReader: false,
            }));

            if (data.results?.type === "catalog_search") {
                setHistoryStack([]);
                pushNewState({
                    results: data.results,
                    showWebReader: false,
                    pdfData: null,
                    linkResults: null,
                    itemId: "",
                });
            } else if (data.results?.type === "item_search" && viewState.itemId) {
                pushNewState({
                    results: data.results,
                    showWebReader: false,
                    pdfData: null,
                    linkResults: null,
                    itemId: viewState.itemId,
                });
            }
        } catch (err: any) {
            console.error("Error sending message:", err);
            setError(err.message || "An unknown error occurred.");

            setMessages((prevMessages) =>
                prevMessages.map((msg) =>
                    msg.id === newUserMessage.id
                        ? { ...msg, status: MessageStatus.Error }
                        : msg
                )
            );
        } finally {
            setIsLoading(false);
        }
    };

    // Streaming message function (per-chunk setMessages updates)
    const sendMessageStream = async (
      text: string,
      onPartial?: (partial: string) => void
    ) => {
      if (!text.trim()) return;

      setError(null);

      const newUserMessage: Message = {
        id: `${Date.now()}-user`,
        data: { content: text },
        status: MessageStatus.Sending,
        type: MessageType.Human,
      };
      setMessages((prev) => [...prev, newUserMessage]);

      // Create a placeholder assistant message that will be populated as chunks arrive
      const assistantId = `${Date.now()}-assistant`;
      setMessages((prev) => [
        ...prev,
        {
          id: assistantId,
          data: { content: "" },
          status: MessageStatus.Sending,
          type: MessageType.Ai,
        },
      ]);

      let textToSend = text;
      if (viewState.itemId !== "") {
        textToSend += `<ItemId>${viewState.itemId}</ItemId>`;
      }

      const messagesForBackend = [
        ...messages.map((msg) => ({
          type: msg.type,
          data: { content: msg.data.content },
        })),
        { type: MessageType.Human, data: { content: textToSend } },
      ];

      let streamedContent = "";
      let finalResults: any = null;

      try {
        const token = localStorage.getItem("authToken");
        const response = await fetch("/api/research-assistant-stream", {
          method: "PUT",
          headers: {
            Authorization: `Basic ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ messages: messagesForBackend }),
        });

        if (response.status === 403) {
          throw new Error("Research Assistant is not enabled in this environment.");
        }
        if (!response.ok) {
          const errorText = await response.text();
          try {
            const errorData = JSON.parse(errorText);
            throw new Error(errorData.error || "Streaming backend error");
          } catch {
            throw new Error(errorText);
          }
        }
        if (!response.body) {
          throw new Error("No response body.");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let done = false;
        let currentChunk = "";

        // Per-chunk UI update
        const pushChunk = async (content: string) => {
          console.log(content);
            setMessages((prev) =>
              prev.map((m) =>
                m.id === assistantId
                  ? { ...m, data: { content }, status: MessageStatus.Sending }
                  : m
              )
            );
        };


        while (!done) {
          const { value, done: streamDone } = await reader.read();
          done = streamDone;

          if (value) {
            currentChunk += decoder.decode(value, { stream: !done });
            const lines = currentChunk.split("\n");
            currentChunk = lines.pop() || ""; // keep possibly incomplete tail

            for (const line of lines) {
              if (!line.trim()) continue;
              try {
                const parsed = JSON.parse(line);

                if (parsed.data?.message) {
                  streamedContent += parsed.data.message;
                  if (onPartial) onPartial(streamedContent);
                  // Immediate UI update per parsed chunk
                  await pushChunk(streamedContent);
                }

                if (parsed.data?.results) {
                  finalResults = parsed.data.results;
                }
              } catch {
                // Ignore malformed lines
              }
            }
          }
        }

        // Finalize statuses
        setMessages((prev) =>
          prev.map((m) => {
            if (m.id === newUserMessage.id) {
              return { ...m, status: MessageStatus.Sent };
            }
            if (m.id === assistantId) {
              return {
                ...m,
                data: { content: streamedContent || "No response provided!!" },
                status: MessageStatus.Sent,
              };
            }
            return m;
          })
        );

        setViewState((prev) => ({
          ...prev,
          results: finalResults,
          showWebReader: false,
        }));

        if (finalResults?.type === "catalog_search") {
          setHistoryStack([]);
          pushNewState({
            results: finalResults,
            showWebReader: false,
            pdfData: null,
            linkResults: null,
            itemId: "",
          });
        } else if (finalResults?.type === "item_search" && viewState.itemId) {
          pushNewState({
            results: finalResults,
            showWebReader: false,
            pdfData: null,
            linkResults: null,
            itemId: viewState.itemId,
          });
        }
      } catch (err: any) {
        console.error("Streaming error:", err);
        setError(err.message || "An unknown error occurred.");
        setMessages((prev) =>
          prev.map((m) =>
            m.id === newUserMessage.id
              ? { ...m, status: MessageStatus.Error }
              : m.id === assistantId
              ? { ...m, status: MessageStatus.Error }
              : m
          )
        );
      }
    };

    const handlePreview = async (url: string) => {
        const urlParts = url.split("/");
        const [itemId, pageId] = [urlParts.at(-3), urlParts.at(-1)];
        const previewItemId = urlParts[urlParts.length - 3];
        const itemsReadResults = await itemsReadFetcher(itemId, pageId);
        setViewState((prev) => ({
            ...prev,
            results: viewState.results,
            pdfData: itemsReadResults.data,
            itemId: previewItemId,
            showWebReader: true,
        }));
        pushNewState({
            results: null,
            showWebReader: true,
            pdfData: itemsReadResults.data,
            linkResults: null,
            itemId: previewItemId,
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
            pdfData: null,
            linkResults: linkResult,
            itemId: "",
        });
    };

    const goToPreviousState = () => {
        setHistoryStack((prevStack) => {
            if (prevStack.length > 1) {
                const prevState = prevStack[prevStack.length - 2];
                setViewState((prev) => ({
                    ...prev,
                    results: prevState.results,
                    itemId: prevState.itemId || "",
                    pdfData: prevState.pdfData,
                    showWebReader: prevState.showWebReader,
                    linkResults: prevState.linkResults,
                }));
                return prevStack.slice(0, -1);
            }

            setViewState((prev) => ({
                ...prev,
                results: null,
                itemId: "",
                pdfData: null,
                showWebReader: false,
                linkResults: null,
            }));
            return [];
        });
    };

    const clearHistory = () => {
        setMessages([]);
        setError(null);
        setViewState((prev) => ({
            ...prev,
            results: null,
            showWebReader: false,
            pdfData: null,
            itemId: "",
            linkResults: null,
        }));
    };

    const value: ResearchAssistantContextType = {
        messages,
        sendMessage,
        sendMessageStream,
        isLoading,
        error,
        historyStack,
        goToPreviousState,
        clearHistory,
        ...viewState,
        setViewState,
        handlePreview,
        handleReadOnline,
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
