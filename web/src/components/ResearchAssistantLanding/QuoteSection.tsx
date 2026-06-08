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
        marginY={{ base: "0px", md: "128px" }}
        paddingX={{ base: "s", sm: "m" }}
        backgroundColor="#FAFDFD"
        alignItems="center"
      >
        <Text
          as="blockquote"
          color="ui.typography.heading"
          fontSize={{
            base: "mobile.heading.heading4",
            md: "desktop.heading.heading3",
          }}
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
            fontSize={{ base: "mobile.body.body1", md: "desktop.body.body1" }}
            fontWeight="bold"
          >
            JENNIFER LOPEZ
          </Text>
          <Text
            color="ui.gray.dark"
            fontSize={{ base: "mobile.body.body1", md: "desktop.body.body1" }}
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
