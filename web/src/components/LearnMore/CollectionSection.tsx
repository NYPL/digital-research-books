import {
  Box,
  Flex,
  Heading,
  Image,
  Link,
  Text,
} from "@nypl/design-system-react-components";
import SectionContainer from "../ResearchAssistantLanding/SectionContainer";
import Divider from "./Divider";

const CollectionSection: React.FC = () => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      textAlign="left"
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Flex flexDir="column" marginBottom="xxl">
          <Heading
            level="h2"
            fontWeight="bold"
            marginBottom="l"
            size="heading2"
            fontFamily="Domine"
          >
            The collection
          </Heading>
          <Flex flexDir="column" gap="s">
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
              <Link
                color="section.research.secondary"
                textDecoration="underline"
                textDecorationStyle="dotted"
                href="https://institutional.org/"
                target="_blank"
                rel="noopener noreferrer"
                _visited={{
                  color: "section.research.secondary",
                }}
              >
                Harvard Institutional Data Initiative
              </Link>
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
          </Flex>
        </Flex>
        <Divider orientation="horizontal" color="#006166" />
        <Flex flexDir="column" marginY="l">
          <Heading
            level="h3"
            marginBottom="s"
            size="heading3"
            fontWeight="400"
            fontFamily="Domine"
          >
            Languages
          </Heading>
          <Text
            fontSize="lg"
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom="l"
          >
            The collection currently contains 136 unique languages. English and
            other Western European languages make up the bulk of the material.
          </Text>
          <Image
            src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/languagesGraph.png"
            alt=""
            width="820px"
            backgroundColor="transparent"
            flexShrink="0"
            margin="0 auto"
          />
        </Flex>
        <Divider orientation="horizontal" color="#006166" />
        <Flex flexDir="column" marginY="l">
          <Heading
            level="h3"
            marginBottom="s"
            size="heading3"
            fontWeight="400"
            fontFamily="Domine"
          >
            Subjects
          </Heading>
          <Text
            fontSize="lg"
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom="l"
          >
            The collection currently contains 82 subjects classified using the
            first level of{" "}
            <Link
              color="section.research.secondary"
              textDecoration="underline"
              textDecorationStyle="dotted"
              href="https://www.loc.gov/catdir/cpso/lcco/"
              target="_blank"
              rel="noopener noreferrer"
              _visited={{
                color: "section.research.secondary",
              }}
            >
              Library of Congress&apos; Classification Outline
            </Link>
            . The table below shows the top ten subjects.
          </Text>
          <Image
            src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/subjectsGraph.png"
            alt=""
            width="820px"
            flexShrink="0"
            backgroundColor="transparent"
            margin="0 auto"
          />
        </Flex>
        <Divider orientation="horizontal" color="#006166" />
        <Flex flexDir="column" marginY="l">
          <Heading
            level="h3"
            marginBottom="s"
            size="heading3"
            fontWeight="400"
            fontFamily="Domine"
          >
            Dates
          </Heading>
          <Text
            fontSize="lg"
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom="l"
          >
            72% of books in the current collection have a precise date of
            publication. Most books were published prior to 1930.
          </Text>
          <Image
            src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/languagesGraph.png"
            alt=""
            width="820px"
            flexShrink="0"
            backgroundColor="transparent"
            margin="0 auto"
          />
        </Flex>
      </Box>
    </SectionContainer>
  );
};

CollectionSection.displayName = "CollectionSection";

export default CollectionSection;
