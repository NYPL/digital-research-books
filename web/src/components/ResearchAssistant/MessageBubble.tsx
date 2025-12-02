import React, { forwardRef } from "react";
import styles from "../../../styles/components/MessageBubble.module.scss";
import { Box, Button, Flex, Text } from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import ThumbsUpIcon from "./ThumbsUpIcon";
import ThumbsDownIcon from "./ThumbsDownIcon";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";

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
        className={`${styles.messageWrapper} ${isUser ? styles.userMessageWrapper : ""
          }`}
      >
        <Box className={bubbleClasses} ref={ref}>
          {isUser ? (
            <Text>
              <b>You: </b>
              {message.data.content}
            </Text>
          ) : (
            <Box display="flex" gap="xs">
              <ResearchAssistantIcon inCircle />
              <Box display="flex" flexDir="column" gap="12px">
                <Box>
                  <Text color="section.research.secondary" isBold>
                    Virtual Research Assistant:
                  </Text>
                  {message.data.content}
                </Box>
                <Flex alignItems="center" justifyContent="space-between">
                  <AiGeneratedText />
                  <Flex>
                    {/* TODO: Add functionality for thumbs up/down buttons */}
                    <Button
                      id="thumbs-up-button"
                      variant="text"
                      aria-label="Thumbs up"
                      padding="xs"
                      minWidth="18px"
                    >
                      <ThumbsUpIcon />
                    </Button>
                    <Button
                      id="thumbs-down-button"
                      variant="text"
                      aria-label="Thumbs down"
                      padding="xs"
                      minWidth="18px"
                    >
                      <ThumbsDownIcon />
                    </Button>
                  </Flex>
                </Flex>
              </Box>
            </Box>
          )}
        </Box>
      </Box>
    );
  }
);

export default MessageBubble;
