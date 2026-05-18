import { Box, Text } from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useEffect, useRef } from "react";
import {
  CATALOG_INITIAL_MESSAGE,
  CONTENT_INITIAL_MESSAGE,
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
  const {
    messages,
    isLoading,
    error,
    results,
    progressEvents,
    searchCompleted: searchCompleted,
  } = useResearchAssistant();

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

  const lastMessage = messages[messages.length - 1];
  // TODO: Replace VRA references with new name
  const getAnnouncementText = () => {
    if (error) return error;
    if (isLoading) return "Virtual Research Assistant is thinking";
    if (
      lastMessage?.type === "message" &&
      lastMessage?.role === MessageRole.Assistant
    ) {
      return `Virtual Research Assistant: ${lastMessage.content
        .map((c) => c.text)
        .join(" ")}`;
    }
    return "";
  };

  const searchStartedEvent = progressEvents?.find(
    (e) => e.type === "search_started"
  );
  const searchContext = searchStartedEvent?.context;

  const loadingMessage: MessageItem = {
    type: ItemType.Message,
    role: MessageRole.Assistant,
    content: [
      {
        text: searchContext ?? "Thinking... This may take several seconds.",
        type: "output_text",
      },
    ],
  };

  return (
    <>
      <Box
        aria-live="polite"
        aria-atomic="true"
        position="absolute"
        width="1px"
        height="1px"
        padding="0"
        margin="-1px"
        overflow="hidden"
        whiteSpace="nowrap"
        borderWidth={0}
        sx={{ clip: "rect(0, 0, 0, 0)" }}
      >
        {getAnnouncementText()}
      </Box>
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
        role="log"
        aria-live="off"
        aria-label="Chat messages"
      >
        <MessageBubble index={0} message={initialMessage} />
        {messages.map((message, index) => {
          if (message.type === "message")
            return (
              <Box
                key={`message-${index}`}
                ref={
                  index === messages.length - 1 && !isLoading
                    ? messagesEndRef
                    : null
                }
              >
                <MessageBubble
                  index={index}
                  message={message}
                  messageResults={results?.[index] ?? null}
                  searchCompletedMessage={searchCompleted?.[index] ?? null}
                />
              </Box>
            );
        })}

        {isLoading && (
          <Box ref={messagesEndRef}>
            <MessageBubble
              index={messages.length}
              message={loadingMessage}
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
    </>
  );
};

export default ResearchAssistantWindow;
