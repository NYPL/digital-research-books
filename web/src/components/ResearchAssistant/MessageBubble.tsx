import { Box, Flex, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import {
  ConversationType,
  MessageItem,
  MessageRole,
} from "~/src/types/ResearchAssistant";
import {
  parseEditionLinks,
  scrollToEdition,
} from "~/src/util/EditionLinkParser";
import { isContentSearchResults } from "~/src/util/ResearchAssistantUtils";
import styles from "../../../styles/components/MessageBubble.module.scss";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import SnippetList from "../ResultCard/SnippetList";
import FeedbackButtons from "./FeedbackButtons";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";

interface MessageBubbleProps {
  message: MessageItem;
  index: number;
}

const MessageBubble = forwardRef<HTMLDivElement, MessageBubbleProps>(
  ({ message, index }, ref) => {
    MessageBubble.displayName = "MessageBubble";

    const isUser = message.role === MessageRole.User;
    const isAssistant = message.role === MessageRole.Assistant;
    const bubbleClasses = isUser
      ? `${styles.messageBubble} ${styles.userBubble}`
      : `${styles.messageBubble} ${styles.assistantBubble}`;

    const handleEditionClick = (editionId: string) => {
      scrollToEdition(editionId);
    };

    const { results, resultType } = useResearchAssistant();

    return (
      <Box
        className={`${styles.messageWrapper} ${
          isUser ? styles.userMessageWrapper : ""
        }`}
      >
        <Box className={bubbleClasses} ref={ref}>
          {isUser ? (
            <Text>
              <b>You: </b>
              {message.content}
            </Text>
          ) : (
            isAssistant &&
            message.content.map((contentItem, idx) => (
              <Flex key={idx} gap="xs" alignItems="flex-start">
                <ResearchAssistantIcon inCircle />
                <Flex flexDir="column" gap="12px">
                  <Box>
                    <Text
                      color="section.research.secondary"
                      isBold
                      display="inline"
                    >
                      VRA:{" "}
                    </Text>
                    {parseEditionLinks(contentItem.text, handleEditionClick)}
                    {resultType === ConversationType.Content &&
                      isContentSearchResults(results) &&
                      results.snippets && (
                        <SnippetList snippets={results.snippets} />
                      )}
                  </Box>
                  {index === 0 ? (
                    <AiGeneratedText isInitial />
                  ) : (
                    <Flex alignItems="center" justifyContent="space-between">
                      <AiGeneratedText />
                      <FeedbackButtons />
                    </Flex>
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

export default MessageBubble;
