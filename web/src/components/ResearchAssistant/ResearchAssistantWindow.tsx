import React, { useEffect, useRef } from "react";
import MessageBubble from "./MessageBubble";
import styles from "../../../styles/components/ResearchAssistantWindow.module.scss";
import { Box, Text } from "@nypl/design-system-react-components";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import {
  getPanelLayout,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";


const ResearchAssistantWindow: React.FC = () => {
  const {
    messages,
    isLoading,
    results,
    showWebReader,
  } = useResearchAssistant();

  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (messagesEndRef.current) {
      const { scrollX, scrollY } = window;
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
      window.scroll(scrollX, scrollY);
    }
  }, [messages]);

  const hasResults =
    (results && Object.keys(results).length > 0) ||
    (showWebReader && !isLoading);
  const { marginX, paddingX, marginRight } = getPanelLayout(hasResults);

  return (
    <Box
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
