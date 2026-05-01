import { Box, Heading, Text } from "@nypl/design-system-react-components";
import SectionContainer from "../ResearchAssistantLanding/SectionContainer";
import Divider from "./Divider";

const CollectionSection: React.FC = () => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      backgroundImage={`
        radial-gradient(circle, rgba(0, 131, 138, 0.025) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      textAlign="left"
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Box display="flex" flexDir="column" marginBottom="xxl">
          <Heading
            level="h2"
            fontWeight="bold"
            marginBottom="l"
            size="heading2"
          >
            <Box display="flex" fontFamily="Domine">
              <span>The collection</span>
            </Box>
          </Heading>
          <Box display="flex" flexDir="column" gap="s">
            <Text
              fontSize="desktop.heading.heading5"
              color="ui.gray.dark"
              fontWeight="medium"
            >
              Digitized Research Books is a collection of over 1 million
              scholarly books published prior to 1930. These come from two
              sources - the New York Public Library&apos;s own collections
              digitized through the Google Books project, and the public corpus
              of the{" "}
              <Text
                as="a"
                color="section.research.secondary"
                textDecoration="underline"
                textDecorationStyle="dotted"
                href="https://institutional.org/"
                target="_blank"
                rel="noopener noreferrer"
              >
                Harvard Institutional Data Initiative
              </Text>
              .
            </Text>
            <Text
              fontSize="desktop.heading.heading5"
              color="ui.gray.dark"
              fontWeight="medium"
            >
              In the future, the collection will also include in-copyright but
              out-of-print books that have been cleared for use by their authors
              and publishers.
            </Text>
            <Text
              fontSize="desktop.heading.heading5"
              color="ui.gray.dark"
              fontWeight="medium"
            >
              More books are added to the collection every month.
            </Text>
          </Box>
        </Box>
        <Divider orientation="horizontal" color="#006166" />
        <Box display="flex" flexDir="column" marginY="l">
          <Heading level="h3" marginBottom="s" size="heading3" fontWeight="400">
            <Box display="flex" fontFamily="Domine">
              <span>Languages</span>
            </Box>
          </Heading>
          <Text
            fontSize="lg"
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom="l"
            fontWeight="regular"
          >
            The collection currently contains 136 unique languages. English and
            other Western European languages make up the bulk of the material.
          </Text>
          {/* change these to be stored online and use link */}
          <img
            src="/images/chart.png"
            alt="Languages in the collection"
            width="822px"
          />
        </Box>
        <Divider orientation="horizontal" color="#006166" />
        <Box display="flex" flexDir="column" marginY="l">
          <Heading level="h3" marginBottom="s" size="heading3" fontWeight="400">
            <Box display="flex" fontFamily="Domine">
              <span>Subjects</span>
            </Box>
          </Heading>
          <Text
            fontSize="lg"
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom="l"
            fontWeight="regular"
          >
            The collection currently contains 82 subjects classified using the
            first level of{" "}
            <Text
              as="a"
              color="section.research.secondary"
              textDecoration="underline"
              textDecorationStyle="dotted"
              href="https://www.loc.gov/catdir/cpso/lcco/"
              target="_blank"
              rel="noopener noreferrer"
            >
              Library of Congress&apos; Classification Outline
            </Text>
            . The table below shows the top ten subjects.
          </Text>
          {/* change these to be stored online and use link */}
          <img
            src="/images/bar.png"
            alt="Subjects in the collection"
            width="822px"
          />
        </Box>
        <Divider orientation="horizontal" color="#006166" />
        <Box display="flex" flexDir="column" marginY="l">
          <Heading level="h3" marginBottom="s" size="heading3" fontWeight="400">
            <Box display="flex" fontFamily="Domine">
              <span>Dates</span>
            </Box>
          </Heading>
          <Text
            fontSize="lg"
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom="l"
            fontWeight="regular"
          >
            72% of books in the current collection have a precise date of
            publication. Most books were published prior to 1930.
          </Text>
          {/* change these to be stored online and use link */}
          <img
            src="/images/line.png"
            alt="Dates in the collection"
            width="822px"
          />
        </Box>
      </Box>
    </SectionContainer>
  );
};

CollectionSection.displayName = "CollectionSection";

export default CollectionSection;
