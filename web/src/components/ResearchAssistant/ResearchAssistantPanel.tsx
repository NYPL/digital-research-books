import React from "react";
import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import RewindIcon from "./icons/RewindIcon";
import {
  getPanelLayout,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import ArrowIcon from "./icons/ArrowIcon";
import { useResultPageContext } from "~/src/context/ResultPageContext";

const ResearchAssistantPanel: React.FC = () => {
  const { results, clearHistory, showWebReader } = useResearchAssistant();

  const { page } = useResultPageContext();

  const hasResults =
    (results && Object.keys(results).length > 0) || showWebReader;

  const { marginX, paddingX, marginRight, paddingRight } = getPanelLayout(
    hasResults
  );

  const hideChat = () => { };

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
      paddingRight={page === "item" ? "l" : paddingRight}
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
          height="40px"
        >
          <ResearchAssistantIcon inCircle />
          <span>Virtual Research Assistant</span>
        </Heading>
        <Flex>
          <Button
            onClick={() => clearHistory(page)}
            variant="text"
            size="small"
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
          <Button
            onClick={hideChat}
            variant="text"
            size="small"
            color="ui.white"
            fontSize="0"
            id="hide-chat-button"
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
              <Text>Hide chat</Text>
              <ArrowIcon color="#FFF" />
            </Flex>
          </Button>
        </Flex>
      </Box>

      <ResearchAssistantWindow />

      <Box
        backgroundColor="section.research.primary"
        position="sticky"
        bottom="0"
        zIndex="1000"
      >
        <ResearchAssistantInput />
      </Box>
    </Box>
  );
};

export default ResearchAssistantPanel;
