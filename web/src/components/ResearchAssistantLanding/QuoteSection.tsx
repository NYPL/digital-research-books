import { Box, Flex, Text } from "@nypl/design-system-react-components";
import SectionContainer from "./SectionContainer";

const FaqSection: React.FC = () => {
  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
    >
      <Flex
        flexDir="column"
        gap="l"
        marginY="128px"
        backgroundColor="#FAFDFD"
        alignItems="center"
      >
        <Text
          as="blockquote"
          color="ui.typography.heading"
          fontSize="desktop.heading.heading3"
          fontFamily="Domine"
          lineHeight="1"
          maxWidth="765px"
        >
          &ldquo;This tool represents a significant leap forward in NYPL&apos;s
          mission to provide world-wide access to scholarly materials.&rdquo;
        </Text>
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
