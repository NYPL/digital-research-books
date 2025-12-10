import React, { forwardRef } from "react";
import styles from "../../../styles/components/MessageBubble.module.scss";
import { Box, Button, Flex, Text } from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";
import ThumbsUpIcon from "./icons/ThumbsUpIcon";
import ThumbsDownIcon from "./icons/ThumbsDownIcon";
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
            <Flex gap="xs" alignItems={message.id === "assistant-initial" ? "center" : "flex-start"}>
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
                {message.id !== "assistant-initial" && (
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
                        sx={{
                          "&:hover": {
                            bgColor: "transparent",
                            svg: {
                              fill: "section.research.primary-05",
                            },
                          },
                        }}
                      >
                        <ThumbsUpIcon />
                      </Button>
                      <Button
                        id="thumbs-down-button"
                        variant="text"
                        aria-label="Thumbs down"
                        padding="xs"
                        minWidth="18px"
                        sx={{
                          "&:hover": {
                            bgColor: "transparent",
                            svg: {
                              fill: "section.research.primary-05",
                            },
                          },
                        }}
                      >
                        <ThumbsDownIcon />
                      </Button>
                    </Flex>
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
