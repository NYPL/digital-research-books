import { Box, Text } from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useEffect, useRef } from "react";
import {
  getPanelLayout,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import {
  ConversationType,
  ItemType,
  MessageItem,
  MessageRole,
} from "~/src/types/ResearchAssistant";
import MessageBubble from "./MessageBubble";

const ResearchAssistantWindow: React.FC = () => {
  const { messages, isLoading, error } = useResearchAssistant();

  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (messagesEndRef.current) {
      const { scrollX, scrollY } = window;
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
      window.scroll(scrollX, scrollY);
    }
  }, [messages]);

  const { marginX, paddingX, marginRight } = getPanelLayout();

  const router = useRouter();
  const conversationType = router.pathname.startsWith("/item/")
    ? ConversationType.Content
    : ConversationType.Catalog;

  const messageText =
    conversationType === ConversationType.Catalog
      ? "What research topic can I help you explore today?"
      : "I can help you find relevant content in this book. Ask me a question, or try the suggestions below.";

  const initialMessage: MessageItem = {
    type: ItemType.Message,
    role: MessageRole.Assistant,
    content: [{ text: messageText, type: "output_text" }],
  };

  return (
    <Box
      flex="1"
      display="flex"
      flexDir="column"
      fontSize="desktop.body.body2"
      overflowY="auto"
      paddingY="s"
      gap="s"
      marginBottom="s"
      marginLeft={marginX}
      marginRight={marginRight}
      paddingLeft={paddingX}
      paddingRight={`calc(${PADDING_COUNTER} * 2)`}
    >
      <MessageBubble index={0} message={initialMessage} ref={null} />
      {messages.map((message, index) => {
        if (message.type === "message")
          return (
            <MessageBubble
              key={`message-${index + 1}`}
              index={index + 1}
              message={message}
              ref={index === messages.length - 1 ? messagesEndRef : null}
            />
          );
      })}

      {isLoading && (
        <MessageBubble
          index={messages.length + 1}
          message={{
            type: ItemType.Message,
            role: MessageRole.Assistant,
            content: [
              {
                text: "Thinking... This may take several seconds.",
                type: "output_text",
              },
            ],
          }}
          ref={messagesEndRef}
          isLoading={isLoading}
        />
      )}

      {error && (
        <Text fontWeight="bold" position="relative" bottom="0">
          {error}
        </Text>
      )}
    </Box>
  );
};

export default ResearchAssistantWindow;
