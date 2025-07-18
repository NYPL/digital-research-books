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
    <div className={styles.windowContainer}>
      {messages.length === 0 && !isLoading && (
        <Box color="white" margin="0 auto">What research topic would you like to explore?</Box>
      )}

      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} />
      ))}

      {isLoading && (
        <Box className={styles.loadingSpinnerContainer}>
          <Box className={styles.loadingSpinner}></Box>
          <Text as="span" className={styles.loadingText}>Assistant thinking...</Text>
        </Box>
      )}
    </div>
  );
};

export default ResearchAssistantWindow;
