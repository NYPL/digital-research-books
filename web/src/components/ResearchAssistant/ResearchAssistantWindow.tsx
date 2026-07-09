import {
  Box,
  Text,
  useNYPLBreakpoints,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useCallback, useEffect, useRef } from "react";
import {
  CATALOG_INITIAL_MESSAGE,
  CONTENT_INITIAL_MESSAGE,
  getPanelLayout,
  ITEM_PAGE_PADDING_RIGHT,
  LOADING_MESSAGE,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { chatAnnouncer } from "~/src/lib/chatAnnouncer/ChatAnnouncer";
import { ConversationType } from "~/src/types/ResearchAssistant";
import { scrollToEdition } from "~/src/util/EditionLinkParser";
import { markdownToPlainText } from "~/src/util/MarkdownParser";
import MessageBubble from "./MessageBubble";

const ResearchAssistantWindow: React.FC = () => {
  const {
    messages,
    isLoading,
    error,
    results,
    showChat,
    toggleChat,
  } = useResearchAssistant();
  const { isLargerThanMedium } = useNYPLBreakpoints();
  const announce = chatAnnouncer.announce;
  const prevMessageCountRef = useRef(0);

  const isLargerThanMediumRef = useRef(isLargerThanMedium);
  useEffect(() => {
    isLargerThanMediumRef.current = isLargerThanMedium;
  }, [isLargerThanMedium]);

  const showChatRef = useRef(showChat);
  useEffect(() => {
    showChatRef.current = showChat;
  }, [showChat]);

  const handleEditionClick = useCallback(
    (editionId: string) => {
      scrollToEdition(editionId);
      if (!isLargerThanMediumRef.current && showChatRef.current) {
        toggleChat();
      }
    },
    [toggleChat]
  );

  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const loadingRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const target = isLoading ? loadingRef.current : messagesEndRef.current;
    target?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [messages, isLoading]);

  const { marginX, paddingX, paddingRight, marginRight } = getPanelLayout();

  const router = useRouter();

  const isItemPage = router.pathname.startsWith("/item/");
  const conversationType = isItemPage
    ? ConversationType.Content
    : ConversationType.Catalog;

  const initialMessage =
    conversationType === ConversationType.Content
      ? CONTENT_INITIAL_MESSAGE
      : CATALOG_INITIAL_MESSAGE;

  // itemPage conditional is here for now, this will be removed with item page responsive PR
  const panelPaddingRight = isItemPage
    ? { base: paddingRight.base, md: ITEM_PAGE_PADDING_RIGHT }
    : paddingRight;

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
        minHeight="0"
        overflowY="auto"
        paddingY="s"
        gap="s"
        marginBottom="s"
        marginLeft={marginX}
        marginRight={marginRight}
        paddingLeft={paddingX}
        paddingRight={panelPaddingRight}
        role="log"
        aria-live="off"
        aria-label="Chat messages"
        sx={{
          overscrollBehavior: "contain",
          WebkitOverflowScrolling: "touch",
          touchAction: "pan-y",
        }}
      >
        <MessageBubble
          index={0}
          message={initialMessage}
          onEditionClick={handleEditionClick}
        />
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
                  onEditionClick={handleEditionClick}
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
              onEditionClick={handleEditionClick}
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
