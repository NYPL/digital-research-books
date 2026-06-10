import { Box, Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";
import UserIcon from "../icons/UserIcon";
import ArrowSVG from "./ArrowSVG";
import InputIcon from "./InputIcon";

const TechnologyPipeline: React.FC = () => {
  const squareSize = { base: "160px", md: "160px" };

  return (
    <>
      <Flex flexDir="column" width="fit-content" margin="0 auto">
        <Flex justifyContent="space-around" width="100%">
          <Flex flexDir="column">
            <Text
              fontSize="desktop.heading.heading8"
              fontWeight="700"
              fontFamily="Courier New"
            >
              QUESTION
            </Text>
            <Box
              background="section.research.secondary"
              border="2px solid"
              borderColor="section.research.secondary"
              borderRadius="0 8px 8px 8px"
            >
              <Box
                boxSizing="content-box"
                width="36px"
                height="36px"
                display="flex"
                flexDir="column"
                alignItems="center"
                borderRadius="50%"
                borderColor="section.research.secondary"
                borderWidth="1rem"
                justifyContent="center"
              >
                <UserIcon size="medium" inCircle={true} />
              </Box>
            </Box>
            <ArrowSVG />
          </Flex>
          <Flex flexDir="column">
            <Text
              fontSize="desktop.heading.heading8"
              fontWeight="700"
              fontFamily="Courier New"
            >
              RESPONSE
            </Text>
            <Box
              background="section.research.secondary"
              border="2px solid"
              borderColor="section.research.secondary"
              borderRadius="8px 8px 0px 8px"
            >
              <Box
                boxSizing="content-box"
                width="36px"
                height="36px"
                borderRadius="50%"
                borderColor="section.research.secondary"
                borderWidth="1rem"
                display="flex"
                alignItems="center"
                justifyContent="center"
              >
                <ResearchAssistantIcon size="medium" inCircle={true} />
              </Box>
            </Box>
            <ArrowSVG />
          </Flex>
        </Flex>
        <Box
          background="ui.white"
          padding="m"
          border="2px solid"
          borderColor="section.research.secondary"
          borderRadius="16px"
          flexDir="column"
          display="flex"
          alignItems="center"
          width="fit-content"
          margin="0 auto"
        >
          <Flex flexDir={{ base: "column", sm: "row" }} alignItems="center">
            <Box
              background="#FAFDFD"
              border="2px dashed"
              borderColor="section.research.secondary"
              borderRadius="16px"
              width={squareSize}
              height={squareSize}
              display="inline-flex"
              flexDir="column"
              alignItems="center"
              justifyContent="center"
              gap="xs"
            >
              <InputIcon />
              <Text
                fontSize="desktop.heading.heading8"
                fontWeight="700"
                fontFamily="Courier New"
                textAlign="center"
              >
                QUESTION <br />
                PROCESSED
              </Text>
            </Box>
            <ArrowSVG />
            <Box
              background="#FAFDFD"
              border="2px dashed"
              borderColor="section.research.secondary"
              borderRadius="16px"
              width={squareSize}
              height={squareSize}
              display="inline-flex"
              flexDir="column"
              alignItems="center"
              justifyContent="center"
              gap="xs"
            >
              <InputIcon />
              <Text
                fontSize="desktop.heading.heading8"
                fontWeight="700"
                fontFamily="Courier New"
                textAlign="center"
              >
                INFORMATION <br /> RECIEVED
              </Text>
            </Box>
            <ArrowSVG />
            <Box
              background="#FAFDFD"
              border="2px dashed"
              borderColor="section.research.secondary"
              borderRadius="16px"
              width={squareSize}
              height={squareSize}
              display="inline-flex"
              flexDir="column"
              alignItems="center"
              justifyContent="center"
              gap="xs"
            >
              <InputIcon />
              <Text
                fontSize="desktop.heading.heading8"
                fontWeight="700"
                fontFamily="Courier New"
                textAlign="center"
              >
                RESPONSE <br />
                GENERATED
              </Text>
            </Box>
          </Flex>
          <Text
            fontSize="desktop.heading.heading8"
            fontWeight="700"
            fontFamily="Courier New"
            marginTop="m"
          >
            AGENTIC AI
          </Text>
        </Box>
      </Flex>
      <Flex flexDir="column" alignItems="center">
        <ArrowSVG />
        <Box
          background="ui.white"
          border="2px solid"
          borderColor="section.research.secondary"
          borderRadius="16px"
          display="inline-flex"
          flexDir="column"
          width={squareSize}
          height={squareSize}
          alignItems="center"
          justifyContent="center"
          gap="xs"
        >
          <InputIcon />
          <Text
            fontSize="desktop.heading.heading8"
            fontWeight="700"
            fontFamily="Courier New"
            textAlign="center"
          >
            BOOK <br />
            INGESTION <br />
            PIPELINE
          </Text>
        </Box>
      </Flex>
    </>
  );
};

export default TechnologyPipeline;
