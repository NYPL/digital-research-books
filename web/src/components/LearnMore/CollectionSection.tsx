import {
  Box,
  Flex,
  Heading,
  Image,
  Text,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import SectionContainer from "../ResearchAssistantLanding/SectionContainer";

const CollectionSection: React.FC = () => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      textAlign="left"
      paddingX="s"
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Flex flexDir="column" marginBottom={{ base: "l", md: "xxl" }}>
          <Heading
            level="h2"
            fontWeight="bold"
            marginBottom={{ base: "s", md: "l" }}
            fontSize={{
              base: "mobile.heading.heading3",
              md: "desktop.heading.heading2",
            }}
            fontFamily="Domine"
          >
            The collection
          </Heading>
          <Flex flexDir="column" gap="s">
            <Text
              fontSize={{
                base: "mobile.heading.heading5",
                md: "desktop.heading.heading5",
              }}
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
                to="https://institutional.org/"
                target="_blank"
                rel="noopener noreferrer"
                _hover={{
                  color: "section.research.primary",
                }}
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
        <Flex
          flexDir="column"
          marginBottom={{ base: "s", md: "l" }}
          paddingTop={{ base: "s", md: "l" }}
          borderTop="1px dashed"
          borderColor="section.research.secondary"
        >
          <Heading
            level="h3"
            marginBottom={{ base: "xs", md: "s" }}
            fontSize={{
              base: "mobile.heading.heading4",
              md: "desktop.heading.heading3",
            }}
            fontWeight="400"
            fontFamily="Domine"
          >
            Languages
          </Heading>
          <Text
            fontSize={{ base: "mobile.subtitle.subtitle", md: "lg" }}
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom={{ base: "none", md: "l" }}
          >
            The collection currently contains 136 unique languages. English and
            other Western European languages make up the bulk of the material.
          </Text>
          <Image
            src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/languagesGraph.png"
            alt="A donut chart displaying a breakdown of languages. English at 42%, Spanish at 19%, Other at 17%, French at 12%, and German at 10%."
            width="820px"
            backgroundColor="transparent"
            flexShrink="0"
            margin="0 auto"
          />
        </Flex>
        <Flex
          flexDir="column"
          marginBottom={{ base: "s", md: "l" }}
          paddingTop={{ base: "s", md: "l" }}
          borderTop="1px dashed"
          borderColor="section.research.secondary"
        >
          <Heading
            level="h3"
            marginBottom={{ base: "xs", md: "s" }}
            fontSize={{
              base: "mobile.heading.heading4",
              md: "desktop.heading.heading3",
            }}
            fontWeight="400"
            fontFamily="Domine"
          >
            Subjects
          </Heading>
          <Text
            fontSize={{ base: "mobile.subtitle.subtitle", md: "lg" }}
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom={{ base: "none", md: "l" }}
          >
            The collection currently contains over 10,000 unique subjects
            classified using the first level of{" "}
            <Link
              color="section.research.secondary"
              to="https://www.loc.gov/catdir/cpso/lcco/"
              target="_blank"
              rel="noopener noreferrer"
              _hover={{
                color: "section.research.primary",
              }}
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
            alt="A horizontal bar chart showing the percentage distribution of subjects. The subjects, ranked from highest to lowest percentage, are: Law at 23%, Fiction at 20%, Poetry at 15%, Periodicals at 10%, Literature at 8%, Engineering at 5%, United States at 4%, and History, Chemistry, and Politics each at 3%."
            width="820px"
            flexShrink="0"
            backgroundColor="transparent"
            margin="0 auto"
          />
        </Flex>
        <Flex
          flexDir="column"
          paddingTop={{ base: "s", md: "l" }}
          borderTop="1px dashed"
          borderColor="section.research.secondary"
        >
          <Heading
            level="h3"
            marginBottom={{ base: "xs", md: "s" }}
            fontSize={{
              base: "mobile.heading.heading4",
              md: "desktop.heading.heading3",
            }}
            fontWeight="400"
            fontFamily="Domine"
          >
            Dates
          </Heading>
          <Text
            fontSize={{ base: "mobile.subtitle.subtitle", md: "lg" }}
            color="ui.gray.dark"
            marginBottom="s"
            paddingBottom={{ base: "none", md: "l" }}
          >
            72% of books in the current collection have a precise date of
            publication. Most books were published prior to 1930.
          </Text>
          <Image
            src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/datesGraph.png"
            alt="A line graph showing the number of published books over time from 1700 to 2000. The volume remains low until the late 1700s, then rises sharply to a peak of over 400,000 books around the year 1890. After this peak, the count drops rapidly, returning to near-zero levels by the mid-1900s."
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
