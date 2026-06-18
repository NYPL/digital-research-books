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
import { chatAnnouncer } from "~/src/lib/chatAnnouncer/ChatAnnouncer";
import { ConversationType } from "~/src/types/ResearchAssistant";
import { markdownToPlainText } from "~/src/util/MarkdownParser";
import MessageBubble from "./MessageBubble";

const ResearchAssistantWindow: React.FC = () => {
  const { messages, isLoading, error, results } = useResearchAssistant();
  const announce = chatAnnouncer.announce;
  const prevMessageCountRef = useRef(0);

  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const loadingRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const target = isLoading ? loadingRef.current : messagesEndRef.current;
    target?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [messages, isLoading]);

  const { marginX, paddingX, marginRight } = getPanelLayout();

  const router = useRouter();

  const conversationType = router.pathname.startsWith("/item/")
    ? ConversationType.Content
    : ConversationType.Catalog;

  const initialMessage =
    conversationType === ConversationType.Content
      ? CONTENT_INITIAL_MESSAGE
      : CATALOG_INITIAL_MESSAGE;

  useEffect(() => {
    const prev = prevMessageCountRef.current;
    const newMessages = messages.slice(prev);

    newMessages.forEach((message) => {
      if (message.type !== "message") return;
      const role = message.role === "assistant" ? "Enhanced Search" : "You";
      const messageContent =
        message.role === "assistant"
          ? message.content
              .map((item) => markdownToPlainText(item.text))
              .join(" ")
          : message.content;
      announce(`${role}: ${messageContent}`);
    });

    prevMessageCountRef.current = messages.length;
  }, [messages, announce]);

  useEffect(() => {
    if (isLoading) announce("Thinking... This may take several seconds.");
  }, [isLoading, announce]);

  useEffect(() => {
    if (error) announce(`Error: ${error}`);
  }, [error, announce]);

  return (
    <>
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
                />
              </Box>
            );
        })}

        {isLoading && (
          <Box ref={loadingRef}>
            <MessageBubble
              index={messages.length}
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
    </>
  );
};

export default ResearchAssistantWindow;
