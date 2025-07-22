import React from "react";
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
  
  return (
    <Box className={styles.windowContainer}>
      {messages.length === 0 && !isLoading && (
        <Box color="white" margin="0 auto">What research topic would you like to explore?</Box>
      )}

      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} />
      ))}

      {isLoading && (
        <Box className={styles.loadingSpinnerContainer}>
          <Box className={styles.loadingSpinner}></Box>
          <Text className={styles.loadingText} noSpace>Assistant thinking...</Text>
        </Box>
      )}
    </Box>
  );
};

export default ResearchAssistantWindow;
