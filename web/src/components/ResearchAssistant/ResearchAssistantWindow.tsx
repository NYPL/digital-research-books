import { Box, Text } from "@nypl/design-system-react-components";
import React, { useEffect, useRef } from "react";
import {
  getPanelLayout,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import styles from "../../../styles/components/ResearchAssistantWindow.module.scss";
import MessageBubble from "./MessageBubble";

const ResearchAssistantWindow: React.FC = () => {
  const { messages, isLoading, error } = useResearchAssistant();

  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (messagesEndRef.current) {
      const { scrollX, scrollY } = window;
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
      window.scroll(scrollX, scrollY);
    }
  }, [messages]);

  const { marginX, paddingX, marginRight } = getPanelLayout();

  return (
    <Box
      flex="1"
      display="flex"
      flexDir="column"
      fontSize="desktop.body.body2"
      overflowY="auto"
      paddingY="s"
      gap="s"
      marginBottom="s"
      marginLeft={marginX}
      marginRight={marginRight}
      paddingLeft={paddingX}
      paddingRight={`calc(${PADDING_COUNTER} * 2)`}
    >
      {messages.map((message, index) => {
        if (message.type === "message")
          return (
            <MessageBubble
              key={`message-${index}`}
              index={index}
              message={message}
              ref={index === messages.length - 1 ? messagesEndRef : null}
            />
          );
      })}

      {isLoading && (
        <Box
          display="flex"
          justifyContent="center"
          alignItems="center"
          paddingX="0"
          paddingY="xs"
        >
          <Box className={styles.loadingSpinner}></Box>
          <Text size="body2" color="ui.white" marginLeft="xs">
            Assistant thinking...
          </Text>
        </Box>
      )}

      {error && (
        <Text fontWeight="bold" position="relative" bottom="0">
          {error}
        </Text>
      )}
    </Box>
  );
};

export default ResearchAssistantWindow;
