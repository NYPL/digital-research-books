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
    <Box
      className={styles.windowContainer}
      display="flex"
      flexDir="column"
      flex="1"
      overflowY="auto"
      paddingX="l"
      paddingY="s"
      gap="s"
      marginBottom="s"
    >
      {messages.length === 0 && !isLoading && (
        <Box color="ui.white" margin="0 auto">
          What research topic would you like to explore?
        </Box>
      )}

      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} />
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
          <Text size="body2" color="ui.white" marginLeft="xs" noSpace>
            Assistant thinking...
          </Text>
        </Box>
      )}
    </Box>
  );
};

export default ResearchAssistantWindow;
