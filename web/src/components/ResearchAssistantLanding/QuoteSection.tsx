import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
import SectionContainer from "./SectionContainer";

const FaqSection: React.FC = () => {
  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderBottom="1px solid"
      borderColor="section.research.primary-10"
    >
      <Flex flexDir="column" gap="l" marginY="128px" backgroundColor="#FAFDFD">
        <Heading size="heading3" fontFamily="Domine" level="h2" lineHeight="1">
          <Text>&quot;This tool represents a significant leap forward in</Text>
          <Text>NYPL&apos;s mission to provide world-wide access to</Text>
          <Text>scholarly materials.&quot;</Text>
        </Heading>
        <Box>
          <Text
            color="section.research.secondary"
            fontSize="desktop.body.body1"
            fontWeight="bold"
          >
            JENNIFER LOPEZ
          </Text>
          <Text
            color="ui.gray.dark"
            fontSize="desktop.body.body1"
            fontWeight="bold"
          >
            CHIEF DIGITAL OFFICER
          </Text>
        </Box>
      </Flex>
    </SectionContainer>
  );
};

export default FaqSection;
