import React from "react";
import styles from "../../../styles/components/MessageBubble.module.scss";
import { Message } from "./useResearchAssistant";
import { Box, Text } from "@nypl/design-system-react-components";

interface MessageBubbleProps {
  message: Message;
}

const MessageBubble: React.FC<MessageBubbleProps> = ({ message }) => {
  const isUser = message.type === "human";
  const bubbleClasses = isUser
    ? `${styles.messageBubble} ${styles.userBubble}`
    : `${styles.messageBubble} ${styles.assistantBubble}`;

  return (
    <Box className={`${styles.messageWrapper} ${isUser ? styles.userMessageWrapper : ''}`}>
      <Box className={bubbleClasses}>
        {!isUser && (
          <Box className={styles.assistantHeader}>
            <span className={styles.assistantIcon}>✨</span> Virtual Research Assistant
          </Box>
        )}
        <Text className={styles.messageContent}>{message.data.content}</Text>
      </Box>
    </Box>
  );
};

export default MessageBubble;
