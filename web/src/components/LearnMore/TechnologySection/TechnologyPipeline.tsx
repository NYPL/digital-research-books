import { Box, Flex, Text } from "@nypl/design-system-react-components";
import React from "react";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";
import UserIcon from "../icons/UserIcon";
import ArrowSVG from "./ArrowSVG";
import BookIcon from "./BookIcon";
import ChatIcon from "./ChatIcon";
import GearIcon from "./GearIcon";
import InfoIcon from "./InfoIcon";

const TechnologyPipeline: React.FC = () => {
  return (
    <>
      <Flex flexDir="column" width="fit-content" margin="0 auto">
        <Flex justifyContent="space-around" width="100%">
          <Flex flexDir="column" alignItems="center">
            <Text
              fontSize="desktop.heading.heading8"
              fontWeight="bold"
              fontFamily="Courier New"
              marginBottom="xs"
            >
              QUESTION
            </Text>
            <Box
              background="section.research.secondary"
              border="2px solid"
              borderColor="section.research.secondary"
              borderRadius="0 8px 8px 8px"
              padding="s"
            >
              <Box width="36px" height="36px">
                <UserIcon size="medium" inCircle={true} />
              </Box>
            </Box>
            <ArrowSVG length="64px" />
          </Flex>
          <Flex flexDir="column" alignItems="center">
            <Text
              fontSize="desktop.heading.heading8"
              fontWeight="bold"
              fontFamily="Courier New"
              marginBottom="xs"
            >
              RESPONSE
            </Text>
            <Box
              background="section.research.secondary"
              border="2px solid"
              borderColor="section.research.secondary"
              borderRadius="8px 8px 0px 8px"
              padding="s"
            >
              <Box
                width="2.25rem"
                height="2.25rem"
                borderRadius="50%"
                backgroundColor="#E6F3F3"
                display="flex"
                alignItems="center"
                justifyContent="center"
              >
                <ResearchAssistantIcon size="large" />
              </Box>
            </Box>
            <ArrowSVG direction="up" length="64px" />
          </Flex>
        </Flex>
        <Box
          background="ui.white"
          padding="m"
          border="2px solid"
          borderColor="section.research.secondary"
          borderRadius="24px"
          flexDir="column"
          display="flex"
          alignItems="center"
          width="fit-content"
          margin="0 auto"
        >
          <Flex flexDir={{ base: "column", md: "row" }} alignItems="center">
            <Text
              fontSize="desktop.heading.heading8"
              fontWeight="bold"
              fontFamily="Courier New"
              marginBottom="m"
              display={{ base: "block", md: "none" }}
            >
              AGENTIC AI
            </Text>
            <Box
              background="#FAFDFD"
              border="2px dashed"
              borderColor="section.research.secondary"
              borderRadius="16px"
              aspectRatio={1}
              display="inline-flex"
              flexDir="column"
              alignItems="center"
              justifyContent="center"
              gap="xs"
            >
              <GearIcon />
              <Text
                fontSize="desktop.heading.heading8"
                fontWeight="bold"
                fontFamily="Courier New"
                textAlign="center"
                width="160px"
              >
                QUESTION <br />
                PROCESSED
              </Text>
            </Box>
            <ArrowSVG
              direction={{ base: "down", md: "right" }}
              length="32px"
              dashed
            />
            <Box
              background="#FAFDFD"
              border="2px dashed"
              borderColor="section.research.secondary"
              borderRadius="16px"
              display="inline-flex"
              flexDir="column"
              alignItems="center"
              justifyContent="center"
              aspectRatio={1}
              gap="xs"
            >
              <InfoIcon />
              <Text
                fontSize="desktop.heading.heading8"
                fontWeight="bold"
                fontFamily="Courier New"
                textAlign="center"
                width="160px"
              >
                INFORMATION <br /> RECIEVED
              </Text>
            </Box>
            <ArrowSVG
              direction={{ base: "down", md: "right" }}
              length="32px"
              dashed
            />
            <Box
              background="#FAFDFD"
              border="2px dashed"
              borderColor="section.research.secondary"
              borderRadius="16px"
              aspectRatio={1}
              display="inline-flex"
              flexDir="column"
              alignItems="center"
              justifyContent="center"
              gap="xs"
            >
              <ChatIcon />
              <Text
                fontSize="desktop.heading.heading8"
                fontWeight="bold"
                fontFamily="Courier New"
                textAlign="center"
                width="160px"
              >
                RESPONSE <br />
                GENERATED
              </Text>
            </Box>
          </Flex>
          <Text
            fontSize="desktop.heading.heading8"
            fontWeight="bold"
            fontFamily="Courier New"
            marginTop="m"
            display={{ base: "none", md: "block" }}
          >
            AGENTIC AI
          </Text>
        </Box>
        <Flex flexDir="column" alignItems="center">
          <ArrowSVG direction="up" length="64px" />
          <Box
            background="ui.white"
            border="2px solid"
            borderColor="section.research.secondary"
            borderRadius="16px"
            display="inline-flex"
            flexDir="column"
            alignItems="center"
            justifyContent="center"
            gap="xs"
            aspectRatio={1}
          >
            <BookIcon />
            <Text
              fontSize="desktop.heading.heading8"
              fontWeight="bold"
              fontFamily="Courier New"
              textAlign="center"
              width="160px"
            >
              BOOK <br />
              INGESTION <br />
              PIPELINE
            </Text>
          </Box>
        </Flex>
      </Flex>
    </>
  );
};

export default TechnologyPipeline;
