import { Box, Heading, Text } from "@nypl/design-system-react-components";
import SectionContainer from "../ResearchAssistantLanding/SectionContainer";
import Divider from "./Divider";

const CollectionSection: React.FC = () => {
  return (
    <SectionContainer
      backgroundImage={`
        radial-gradient(circle, rgba(0, 131, 138, 0.025) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      display="flex"
      flexDir="column"
      textAlign="left"
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Box display="flex" flexDir="column" marginBottom="m">
          <Heading
            level="h3"
            fontWeight="bold"
            marginBottom="s"
            size="heading3"
          >
            <Box display="flex" fontFamily="Domine">
              <span>The collection</span>
            </Box>
          </Heading>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            fontWeight="semibold"
            marginBottom="s"
          >
            Digitized Research Books is a collection of over 1 million scholarly
            books published prior to 1930. These come from two sources - the New
            York Public Library&apos;s own collections digitized through the
            Google Books project, and the public corpus of the{" "}
            <a
              color="section.research.secondary"
              href="https://institutional.org/"
              target="_blank"
              rel="noopener noreferrer"
            >
              Harvard Institutional Data Initiative
            </a>
            .
          </Text>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            fontWeight="semibold"
            marginBottom="s"
          >
            In the future, the collection will also include in-copyright but
            out-of-print books that have been cleared for use by their authors
            and publishers.
          </Text>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            fontWeight="semibold"
            marginBottom="s"
          >
            More books are added to the collection every month.
          </Text>
        </Box>
        <Divider orientation="horizontal" color="#006166" />
        <Box display="flex" flexDir="column" marginTop="m" marginBottom="m">
          <Heading level="h3" marginBottom="s" size="heading3">
            <Box display="flex" fontFamily="Domine">
              <span>Languages</span>
            </Box>
          </Heading>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            marginBottom="s"
          >
            The collection currently contains 136 unique languages. English and
            other Western European languages make up the bulk of the material.
          </Text>
        </Box>
        <Divider orientation="horizontal" color="#006166" />
        <Box display="flex" flexDir="column" marginTop="m" marginBottom="m">
          <Heading level="h3" marginBottom="s" size="heading3">
            <Box display="flex" fontFamily="Domine">
              <span>Subjects</span>
            </Box>
          </Heading>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            marginBottom="s"
          >
            The collection currently contains 82 subjects classified using the
            first level of{" "}
            <a
              color="section.research.secondary"
              href="https://www.loc.gov/catdir/cpso/lcco/"
              target="_blank"
              rel="noopener noreferrer"
            >
              Library of Congress&apos; Classification Outline
            </a>
            . The table below shows the top ten subjects.
          </Text>
        </Box>
        <Divider orientation="horizontal" color="#006166" />
        <Box display="flex" flexDir="column" marginTop="m" marginBottom="m">
          <Heading level="h3" marginBottom="s" size="heading3">
            <Box display="flex" fontFamily="Domine">
              <span>Dates</span>
            </Box>
          </Heading>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            marginBottom="s"
          >
            72% of books in the current collection have a precise date of
            publication. Most books were published prior to 1930.
          </Text>
        </Box>
      </Box>
    </SectionContainer>
  );
};

CollectionSection.displayName = "CollectionSection";

export default CollectionSection;
