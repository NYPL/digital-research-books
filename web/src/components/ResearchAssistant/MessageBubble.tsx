import React, { forwardRef } from "react";
import styles from "../../../styles/components/MessageBubble.module.scss";
import { Box, Text } from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

interface MessageBubbleProps {
  message: Message;
}

const MessageBubble = forwardRef<HTMLDivElement, MessageBubbleProps>(
  ({ message }, ref) => {
    const isUser = message.type === "human";
    const bubbleClasses = isUser
      ? `${styles.messageBubble} ${styles.userBubble}`
      : `${styles.messageBubble} ${styles.assistantBubble}`;

    return (
      <Box
        className={`${styles.messageWrapper} ${isUser ? styles.userMessageWrapper : ""
          }`}
      >
        <Box className={bubbleClasses} ref={ref}>
          {isUser ? (
            <Text className={styles.messageContent} noSpace>
              <b>You: </b>
              {message.data.content}
            </Text>
          ) : (
            <Box className={styles.messageContent} display="flex" gap="xs">
              <ResearchAssistantIcon />
              <Box display="flex" flexDir="column" gap="m">
                <Box>
                  <Text color="section.research.primary" isBold noSpace>
                    Virtual Research Assistant:
                  </Text>
                  {message.data.content}
                </Box>
                <Text color="ui.gray.semi-dark" noSpace>
                  AI-generated. Verify results.
                </Text>
              </Box>
            </Box>
          )}
        </Box>
      </Box>
    );
  }
);

export default MessageBubble;
