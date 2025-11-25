import React, { useEffect, useRef } from "react";
import MessageBubble from "./MessageBubble";
import styles from "../../../styles/components/ResearchAssistantWindow.module.scss";
import { Box, Text } from "@nypl/design-system-react-components";
import { Message } from "~/src/types/ResearchAssistant";

interface ResearchAssistantWindowProps {
  messages: Message[];
  isLoading: boolean;
}

const ResearchAssistantWindow: React.FC<ResearchAssistantWindowProps> = ({
  messages,
  isLoading,
}) => {
  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (messagesEndRef.current) {
      const { scrollX, scrollY } = window;
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
      window.scroll(scrollX, scrollY);
    }
  }, [messages]);

  return (
    <Box
      display="flex"
      flexDir="column"
      fontSize="desktop.body.body2"
      overflowY="auto"
      paddingLeft="l"
      paddingY="s"
      gap="s"
      marginBottom="s"
    >
      {messages.map((message, index) => (
        <MessageBubble
          key={message.id}
          message={message}
          ref={index === messages.length - 1 ? messagesEndRef : null}
        />
      ))}

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
    </Box>
  );
};

export default ResearchAssistantWindow;
