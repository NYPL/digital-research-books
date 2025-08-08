import React, {
    createContext,
    useContext,
    useState,
    useCallback,
    useMemo,
} from "react";
import {
    ChatResults,
    ApiItemsRead,
    Message,
    MessageStatus,
    MessageType,
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
    handleCloseReader: () => void;
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
    const [showWebReader, setShowWebReader] = useState(false);
    const [itemId, setItemId] = useState<string>("");
    const [pdfData, setPdfData] = useState<ApiItemsRead | null>(null);
    const [linkResults, setLinkResults] = useState<LinkResult | null>(null);

    const sendMessage = useCallback(
        async (text: string) => {
            if (!text.trim()) return;

            setError(null);

            if (itemId !== "") {
                text += `<ItemId>${itemId}</ItemId>`;
            }

            const newUserMessage: Message = {
                id: Date.now().toString() + "-user",
                data: { content: text },
                status: MessageStatus.Sending,
                type: MessageType.Human,
            };
            setMessages((prevMessages) => [...prevMessages, newUserMessage]);
            setIsLoading(true);

            const messagesForBackend = [
                ...messages.map((msg) => ({
                    type: msg.type,
                    data: { content: msg.data.content },
                })),
                { type: MessageType.Human, data: { content: text } },
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
                const assistantResponseContent = data.answer || "No response provided.";

                const assistantMessage: Message = {
                    id: Date.now().toString() + "-assistant",
                    data: {
                        content: assistantResponseContent,
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
        },
        [messages]
    );

    const handleCloseReader = useCallback(() => {
        setShowWebReader(false);
        setPdfData(null);
        setLinkResults(null);
        setItemId("");
    }, []);

    const handlePreview = useCallback(async (url: string) => {
        const urlParts = url.split("/");
        const previewItemId = urlParts[urlParts.length - 3];
        const itemsReadResults = await itemsReadFetcher(
            previewItemId,
            urlParts[urlParts.length - 1]
        );
        setPdfData(itemsReadResults.data);
        setItemId(previewItemId);
        setShowWebReader(true);
    }, []);

    const handleReadOnline = useCallback(async (linkId: number) => {
        const linkResult = await readFetcher(linkId);
        setLinkResults(linkResult);
        setShowWebReader(true);
    }, []);

    const clearHistory = useCallback(() => {
        setMessages([]);
        setResults(null);
        setError(null);
        handleCloseReader();
    }, [handleCloseReader]);

    const value = useMemo(
        () => ({
            messages,
            sendMessage,
            results,
            setResults,
            isLoading,
            error,
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
            handleCloseReader,
        }),
        [
            messages,
            results,
            isLoading,
            error,
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
            handleCloseReader,
            sendMessage,
        ]
    );

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
