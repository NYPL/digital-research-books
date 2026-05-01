import { Box, Text } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "../../ResearchAssistant/icons/ResearchAssistantIcon";
import BookIcon from "../icons/BookIcon";
import UserIcon from "../icons/UserIcon";

const MissionDiagram = () => {
  return (
    <Box
      display="flex"
      justifyContent="center"
      flexDir="column"
      marginTop="xxxl"
      marginBottom="xl"
    >
      <Box
        display="flex"
        flexDir="row"
        justifyContent="center"
        marginBottom="s"
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
        >
          <UserIcon size="medium" inCircle={true} />
        </Box>
        <Box
          width="13rem"
          height="1px"
          borderColor="section.research.secondary"
          borderWidth="2px"
          margin="auto 0"
        ></Box>
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
          >
            <ResearchAssistantIcon size="large" />
          </Box>
        </Box>
        <Box
          width="13rem"
          height="1px"
          borderColor="section.research.secondary"
          borderWidth="2px"
          margin="auto 0"
        ></Box>
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
        >
          <BookIcon inCircle={true} />
        </Box>
      </Box>
      <Box
        display="flex"
        flexDir="row"
        width="100%"
        justifyContent="space-between"
        margin="0 auto"
        maxWidth="833px"
      >
        <Box flex="1">
          <Text fontWeight="bold">USER</Text>
        </Box>
        <Box flex="1">
          <Text fontWeight="bold">ENHANCED SEARCH</Text>
          <Text color="ui.gray.x-dark">The technology</Text>
        </Box>
        <Box flex="1">
          <Text fontWeight="bold">DIGITIZED RESEARCH BOOKS</Text>
          <Text color="ui.gray.x-dark">The collection</Text>
        </Box>
      </Box>
    </Box>
  );
};

export default MissionDiagram;
