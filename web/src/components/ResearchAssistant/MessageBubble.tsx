import { Box, Flex, Text } from "@nypl/design-system-react-components";
import { memo } from "react";
import {
  ChatResults,
  MessageItem,
  MessageRole,
} from "~/src/types/ResearchAssistant";
import { scrollToEdition } from "~/src/util/EditionLinkParser";
import { renderMarkdownContent } from "~/src/util/MarkdownParser";
import { isContentSearchResults } from "~/src/util/ResearchAssistantUtils";
import styles from "../../../styles/components/MessageBubble.module.scss";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import HiddenAria from "../HiddenAria/HiddenAria";
import SnippetList from "../ResultCard/SnippetList";
import FeedbackButtons from "./FeedbackButtons";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";
import LoadingEllipses from "./LoadingEllipses";

interface MessageBubbleProps {
  message: MessageItem;
  index: number;
  isLoading?: boolean;
  messageResults?: ChatResults | null;
}

const MessageBubble = memo(
  ({ message, index, isLoading, messageResults }: MessageBubbleProps) => {
    const isUser = message.role === MessageRole.User;
    const isAssistant = message.role === MessageRole.Assistant;
    const bubbleClasses = isUser
      ? `${styles.messageBubble} ${styles.userBubble}`
      : `${styles.messageBubble} ${styles.assistantBubble}`;

    const handleEditionClick = (editionId: string) => {
      scrollToEdition(editionId);
    };

    return (
      <Box
        className={`${styles.messageWrapper} ${
          isUser ? styles.userMessageWrapper : ""
        }`}
      >
        <Box className={bubbleClasses}>
          {isUser ? (
            <Text>
              <b>You: </b>
              {message.content}
            </Text>
          ) : (
            isAssistant &&
            message.content.map((contentItem, idx) => (
              <Flex
                key={idx}
                gap="xs"
                alignItems={isLoading ? "center" : "flex-start"}
                data-testid="assistant-message-bubble"
              >
                {isLoading ? (
                  <LoadingEllipses />
                ) : (
                  <ResearchAssistantIcon inCircle />
                )}
                <Flex flexDir="column" gap="12px">
                  <Box>
                    <HiddenAria>Enhanced Search says:</HiddenAria>
                    {renderMarkdownContent(
                      contentItem.text,
                      handleEditionClick
                    )}
                  </Box>
                  {!isLoading &&
                    (index === 0 ? (
                      <AiGeneratedText isInitial />
                    ) : (
                      <Flex alignItems="center" justifyContent="space-between">
                        <AiGeneratedText />
                        <FeedbackButtons label="message feedback" />
                      </Flex>
                    ))}
                  {isContentSearchResults(messageResults) &&
                    messageResults.snippets && (
                      <SnippetList snippets={messageResults.snippets} />
                    )}
                </Flex>
              </Flex>
            ))
          )}
        </Box>
      </Box>
    );
  }
);

MessageBubble.displayName = "MessageBubble";

export default MessageBubble;
