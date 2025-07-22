import React from "react";
import styles from "../../../styles/components/MessageBubble.module.scss";
import { Box, Text } from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

interface MessageBubbleProps {
  message: Message;
}

const MessageBubble: React.FC<MessageBubbleProps> = ({ message }) => {
  const isUser = message.type === "human";
  const bubbleClasses = isUser
    ? `${styles.messageBubble} ${styles.userBubble}`
    : `${styles.messageBubble} ${styles.assistantBubble}`;

  return (
    <Box
      className={`${styles.messageWrapper} ${isUser ? styles.userMessageWrapper : ""}`}
    >
      <Box className={bubbleClasses}>
        {isUser ? (
          <Text className={styles.messageContent} as="div">
            <b>You: </b>
            {message.data.content}
          </Text>
        ) : (
          <Text className={styles.messageContent} as="div">
            <Box display="flex" gap="xs">
              <ResearchAssistantIcon />
              <Box display="flex" flexDir="column" gap="1.5rem">
                <Box>
                  <b>
                    <Text color="section.research.primary" noSpace>
                      Virtual Research Assistant:{" "}
                    </Text>
                  </b>
                  {message.data.content}
                </Box>
                <Text color="ui.gray.semi-dark" as="div">AI-generated. Verify results.</Text>
              </Box>
            </Box>
          </Text>
        )}
      </Box>
    </Box>
  );
};

export default MessageBubble;
