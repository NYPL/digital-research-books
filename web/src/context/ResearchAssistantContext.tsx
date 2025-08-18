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

interface ResearchAssistantContextType {
    messages: any[];
    sendMessage: (message: string) => Promise<void>;
    results: ChatResults | null;
    setResults: React.Dispatch<React.SetStateAction<ChatResults | null>>;
    isLoading: boolean;
    error: string | null;
    historyStack: HistoryItem[];
    goToPreviousState: () => void;
    clearHistory: () => void;
    showWebReader: boolean;
    setShowWebReader: React.Dispatch<React.SetStateAction<boolean>>;
    itemId: string;
    setItemId: React.Dispatch<React.SetStateAction<string>>;
    pdfData: ApiItemsRead | null;
    setPdfData: React.Dispatch<React.SetStateAction<ApiItemsRead | null>>;
    linkResults: LinkResult | null;
    setLinkResults: React.Dispatch<React.SetStateAction<LinkResult | null>>;
    handlePreview: (url: string) => Promise<void>;
    handleReadOnline: (linkId: number) => Promise<void>;
}

const ResearchAssistantContext = createContext<
    ResearchAssistantContextType | undefined
>(undefined);

export const ResearchAssistantProvider: React.FC<{
    children: React.ReactNode;
}> = ({ children }) => {
    const [messages, setMessages] = useState<any[]>([]);
    const [results, setResults] = useState<ChatResults | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const [historyStack, setHistoryStack] = useState<HistoryItem[]>([]);

    const [showWebReader, setShowWebReader] = useState(false);
    const [itemId, setItemId] = useState<string>("");
    const [pdfData, setPdfData] = useState<ApiItemsRead | null>(null);
    const [linkResults, setLinkResults] = useState<LinkResult | null>(null);

    const pushNewState = (
        results: ChatResults,
        showWebReader: boolean,
        pdfData: ApiItemsRead | null,
        linkResults: LinkResult | null,
        itemId?: string
    ) => {
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
        if (itemId !== "") {
            textToSend += `<ItemId>${itemId}</ItemId>`;
        }

        const messagesForBackend = [
            ...messages.map((msg) => ({
                type: msg.type,
                data: { content: msg.data.content },
            })),
            { type: MessageType.Human, data: { content: textToSend } },
        ];

        try {
            const response = await fetch("/api/research-assistant", {
                method: "PUT",
                headers: {
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

            setResults(data.results);
            setShowWebReader(false);

            if (data.results?.type === "catalog_search") {
                setHistoryStack([]);
                pushNewState(data.results, false, null, null, "");
            } else if (data.results?.type === "item_search" && itemId) {
                pushNewState(data.results, false, null, null, itemId);
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

    const handlePreview = async (url: string) => {
        const urlParts = url.split("/");
        const previewItemId = urlParts[urlParts.length - 3];
        const itemsReadResults = await itemsReadFetcher(
            previewItemId,
            urlParts[urlParts.length - 1]
        );
        setPdfData(itemsReadResults.data);
        setItemId(previewItemId);
        setShowWebReader(true);
        pushNewState(null, true, itemsReadResults.data, null, previewItemId);
    };

    const handleReadOnline = async (linkId: number) => {
        const linkResult = await readFetcher(linkId);
        setLinkResults(linkResult);
        setShowWebReader(true);
        pushNewState(null, true, null, linkResult, "");
    };

    const goToPreviousState = () => {
        setHistoryStack((prevStack) => {
            if (prevStack.length > 1) {
                const prevState = prevStack[prevStack.length - 2];
                setResults(prevState.results);
                setItemId(prevState.itemId || "");
                setShowWebReader(prevState.showWebReader);
                setPdfData(prevState.pdfData);
                setLinkResults(prevState.linkResults);
                return prevStack.slice(0, -1);
            }

            setResults(null);
            setItemId("");
            setShowWebReader(false);
            setPdfData(null);
            setLinkResults(null);
            return [];
        });
    };

    const clearHistory = () => {
        setMessages([]);
        setResults(null);
        setError(null);
        setShowWebReader(false);
        setPdfData(null);
        setLinkResults(null);
        setItemId("");
    };

    const value: ResearchAssistantContextType = {
        messages,
        sendMessage,
        results,
        setResults,
        isLoading,
        error,
        historyStack,
        goToPreviousState,
        clearHistory,
        showWebReader,
        setShowWebReader,
        itemId,
        setItemId,
        pdfData,
        setPdfData,
        linkResults,
        setLinkResults,
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
