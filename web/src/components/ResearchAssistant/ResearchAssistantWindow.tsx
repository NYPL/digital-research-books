import { Box, Text } from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useEffect, useRef } from "react";
import {
  CATALOG_INITIAL_MESSAGE,
  CONTENT_INITIAL_MESSAGE,
  getPanelLayout,
  LOADING_MESSAGE,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { ConversationType } from "~/src/types/ResearchAssistant";
import MessageBubble from "./MessageBubble";

const ResearchAssistantWindow: React.FC = () => {
  const { messages, isLoading, error, results } = useResearchAssistant();

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

  const initialMessage =
    conversationType === ConversationType.Content
      ? CONTENT_INITIAL_MESSAGE
      : CATALOG_INITIAL_MESSAGE;

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
      <MessageBubble index={0} message={initialMessage} />
      {messages.map((message, index) => {
        if (message.type === "message")
          return (
            <Box
              key={`message-${index + 1}`}
              ref={
                index === messages.length - 1 && !isLoading
                  ? messagesEndRef
                  : null
              }
            >
              <MessageBubble
                index={index + 1}
                message={message}
                messageResults={results?.[index + 1] ?? null}
              />
            </Box>
          );
      })}

      {isLoading && (
        <Box ref={messagesEndRef}>
          <MessageBubble
            index={messages.length + 1}
            message={LOADING_MESSAGE}
            isLoading={isLoading}
          />
        </Box>
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
