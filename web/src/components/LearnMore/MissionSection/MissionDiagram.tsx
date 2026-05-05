import { Box, Flex, Text } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";
import BookIcon from "../icons/BookIcon";
import UserIcon from "../icons/UserIcon";

const MissionDiagram = () => {
  return (
    <Flex
      position="relative"
      justifyContent="center"
      alignItems="flex-start"
      flexDir="row"
      marginTop="xxxl"
      marginBottom="xl"
    >
      <Box
        position="absolute"
        top="26%"
        left="50%"
        right="0"
        height="1px"
        maxWidth="500px"
        width="70%"
        border="solid 2px"
        borderColor="section.research.secondary"
        transform="translateY(-50%) translateX(-50%)"
        zIndex={0}
      />
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
          <Text color="ui.gray.x-dark">The technology</Text>
        </Box>
      </Flex>
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
          <Text color="ui.gray.x-dark">The collection</Text>
        </Box>
      </Flex>
    </Flex>
  );
};

export default MissionDiagram;
