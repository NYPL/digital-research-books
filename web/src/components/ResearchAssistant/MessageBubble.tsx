import { Box, Flex, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { MessageItem, MessageRole } from "~/src/types/ResearchAssistant";
import {
  parseEditionLinks,
  scrollToEdition,
} from "~/src/util/EditionLinkParser";
import styles from "../../../styles/components/MessageBubble.module.scss";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import FeedbackButtons from "./FeedbackButtons";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";
import LoadingEllipses from "./LoadingEllipses";

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

    const { isLoading } = useResearchAssistant();

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
              <Flex
                key={idx}
                gap="xs"
                alignItems={isLoading ? "center" : "flex-start"}
              >
                {isLoading ? (
                  <LoadingEllipses />
                ) : (
                  <ResearchAssistantIcon inCircle />
                )}
                <Flex flexDir="column" gap="12px">
                  <Box>
                    {!isLoading && (
                      <Text
                        color="section.research.secondary"
                        isBold
                        display="inline"
                      >
                        VRA:{" "}
                      </Text>
                    )}
                    {parseEditionLinks(contentItem.text, handleEditionClick)}
                  </Box>
                  {!isLoading &&
                    (index === 0 ? (
                      <AiGeneratedText isInitial />
                    ) : (
                      <Flex alignItems="center" justifyContent="space-between">
                        <AiGeneratedText />
                        <FeedbackButtons />
                      </Flex>
                    ))}
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
