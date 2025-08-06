import { useState, useCallback } from "react";
import {
  UseResearchAssistantResult,
  Message,
  ChatResults,
  MessageStatus,
  MessageType,
} from "~/src/types/ResearchAssistant";

export const useResearchAssistant = (): UseResearchAssistantResult => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [results, setResults] = useState<ChatResults>();
  const [itemId, setItemId] = useState<string>("");
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const sendMessage = useCallback(
    async (text: string) => {
      if (!text.trim()) return;

      setError(null);

      console.log(itemId)
      if (itemId !== "") {
        text +=(`<item_id=${itemId}>`);
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

  const clearHistory = useCallback(() => {
    setMessages([]);
    setError(null);
  }, []);

  return { messages, sendMessage, itemId, setItemId, results, setResults, isLoading, error, clearHistory };
};
