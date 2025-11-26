import React from "react";
import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import RewindIcon from "./RewindIcon";
import {
  getPanelLayout,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";

const ResearchAssistantPanel: React.FC = () => {
  const {
    isLoading,
    results,
    error,
    clearHistory,
    showWebReader,
  } = useResearchAssistant();

  const hasResults =
    (results && Object.keys(results).length > 0) || (showWebReader && !isLoading);
  const { marginX, paddingX, marginRight } = getPanelLayout(hasResults);

  return (
    <Box
      flex="1"
      display="flex"
      flexDirection="column"
      bgColor="section.research.primary"
      maxHeight="100vh"
      position="sticky"
      top="0"
      width="100%"
      paddingLeft={paddingX}
      paddingRight={paddingX}
    >
      <Box
        bgColor="section.research.primary"
        display="flex"
        justifyContent="space-between"
        alignItems="center"
        borderBottom="1px white solid"
        marginLeft={marginX}
        marginRight={marginRight}
        paddingLeft={paddingX}
        paddingRight={`calc(${PADDING_COUNTER} * 2)`}
        position="sticky"
        paddingY="s"
        top="0"
        zIndex="999"
      >
        <Heading
          level="h2"
          size="heading7"
          color="ui.white"
          display="flex"
          alignItems="center"
          gap="xs"
        >
          <ResearchAssistantIcon inCircle />
          <span>Virtual Research Assistant</span>
        </Heading>
        <Button
          onClick={clearHistory}
          variant="text"
          color="ui.white"
          fontSize="0"
          id="clear-history-button"
          sx={{
            "&:hover": {
              color: "ui.link.secondary",
              path: {
                stroke: "ui.link.secondary",
              },
            },
          }}
        >
          <Flex gap="xxs" alignItems="center">
            <RewindIcon /> <Text>Start over</Text>
          </Flex>
        </Button>
      </Box>

      <ResearchAssistantWindow />

      {error && <Text fontWeight="bold">{error}</Text>}

      <ResearchAssistantInput />
    </Box>
  );
};

export default ResearchAssistantPanel;
