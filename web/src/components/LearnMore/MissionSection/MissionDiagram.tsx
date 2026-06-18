import { Box, Flex, Text } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";
import BookIcon from "../icons/BookIcon";
import UserIcon from "../icons/UserIcon";

const MissionDiagram = () => {
  return (
    <Flex
      position="relative"
      justifyContent="center"
      alignItems={{ base: "center", sm: "flex-start" }}
      flexDir={{ base: "column", sm: "row" }}
      marginTop={{ base: "xl", sm: "xxl", md: "xxxl" }}
      marginBottom={{ base: "l", md: "xl" }}
    >
      <Box
        position="absolute"
        top="22%"
        left="50%"
        width="70%"
        borderColor="section.research.secondary"
        transform="translateY(-50%) translateX(-50%)"
        zIndex={0}
        display={{ base: "none", sm: "block" }}
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          style={{ display: "block" }}
          width="100%"
          height="2"
          viewBox="0 0 500 2"
          fill="none"
        >
          <path d="M500 1L0 1" stroke="#006166" strokeWidth="2" />
        </svg>
      </Box>

      <Flex alignItems="center" flexDir="column" width="239px" zIndex={1}>
        <Box
          boxSizing="content-box"
          width="2.25rem"
          height="2.25rem"
          display="flex"
          flexDir="column"
          alignItems="center"
          borderRadius="50%"
          borderColor="section.research.secondary"
          borderWidth="1rem"
          justifyContent="center"
          marginBottom="s"
        >
          <UserIcon size="medium" inCircle={true} />
        </Box>
        <Text fontWeight="bold">USER</Text>
      </Flex>
      <Box display={{ base: "block", sm: "none" }} lineHeight="0">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          style={{ display: "block" }}
          width="2"
          height="60"
          viewBox="0 0 2 60"
          fill="none"
        >
          <path d="M1 60L1 0" stroke="#006166" strokeWidth="2" />
        </svg>
      </Box>

      <Flex
        justifyContent="center"
        alignItems="center"
        flexDir="column"
        width="239px"
        zIndex={1}
      >
        <Box display="flex" flexDir="column" alignItems="center">
          <Box
            boxSizing="content-box"
            width="2.25rem"
            height="2.25rem"
            borderRadius="50%"
            backgroundColor="#F9E08E"
            borderColor="section.research.secondary"
            borderWidth="1rem"
            display="flex"
            alignItems="center"
            justifyContent="center"
            marginBottom="s"
          >
            <ResearchAssistantIcon size="large" />
          </Box>
        </Box>
        <Box flex="1">
          <Text fontWeight="bold">ENHANCED SEARCH</Text>
          <Text color="ui.gray.x-dark" fontWeight="regular" isItalic>
            The technology
          </Text>
        </Box>
      </Flex>
      <Box display={{ base: "block", sm: "none" }} lineHeight="0">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          style={{ display: "block" }}
          width="2"
          height="60"
          viewBox="0 0 2 60"
          fill="none"
        >
          <path d="M1 60L1 0" stroke="#006166" strokeWidth="2" />
        </svg>
      </Box>
      <Flex
        justifyContent="center"
        alignItems="center"
        flexDir="column"
        width="239px"
        zIndex={1}
      >
        <Box
          boxSizing="content-box"
          width="2.25rem"
          height="2.25rem"
          display="flex"
          flexDir="column"
          alignItems="center"
          borderRadius="50%"
          borderColor="section.research.secondary"
          borderWidth="1rem"
          justifyContent="center"
          marginBottom="s"
        >
          <BookIcon inCircle={true} />
        </Box>
        <Box flex="1">
          <Text fontWeight="bold">DIGITIZED RESEARCH BOOKS</Text>
          <Text color="ui.gray.x-dark" fontWeight="regular" isItalic>
            The collection
          </Text>
        </Box>
      </Flex>
    </Flex>
  );
};

export default MissionDiagram;
