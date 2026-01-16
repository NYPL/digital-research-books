import { Box, Flex, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import { Message } from "~/src/types/ResearchAssistant";
import styles from "../../../styles/components/MessageBubble.module.scss";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import FeedbackButtons from "./FeedbackButtons";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";

interface MessageBubbleProps {
  message: Message;
}

const MessageBubble = forwardRef<HTMLDivElement, MessageBubbleProps>(
  ({ message }, ref) => {
    MessageBubble.displayName = "MessageBubble";

    const isUser = message.type === "human";
    const bubbleClasses = isUser
      ? `${styles.messageBubble} ${styles.userBubble}`
      : `${styles.messageBubble} ${styles.assistantBubble}`;

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
              {message.data.content}
            </Text>
          ) : (
            <Flex gap="xs" alignItems="flex-start">
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
                  {message.data.content}
                </Box>
                {message.id === "assistant-initial" ? (
                  <AiGeneratedText isInitial />
                ) : (
                  <Flex alignItems="center" justifyContent="space-between">
                    <AiGeneratedText />
                    <FeedbackButtons />
                  </Flex>
                )}
              </Flex>
            </Flex>
          )}
        </Box>
      </Box>
    );
  }
);

export default MessageBubble;
