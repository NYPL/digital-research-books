import {
  Flex,
  Heading,
  Text,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import { ASK_NYPL } from "~/src/constants/links";
import Link from "../../Link/Link";
import ArrowIcon from "../../ResearchAssistant/icons/ArrowIcon";
import LandingButtons from "../LandingButtons";
import LandingCard from "../LandingCard";
import SectionContainer from "../SectionContainer";
import LightbulbIcon from "./LightbulbIcon";
import MailIcon from "./MailIcon";

interface HelpSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
  textInputRef: React.RefObject<TextInputRefType>;
}

const HelpSection: React.FC<HelpSectionProps> = ({
  heroSectionRef,
  textInputRef,
}) => {
  return (
    <SectionContainer
      backgroundImage={`
        radial-gradient(circle, rgba(0, 131, 138, 0.025) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      alignItems="center"
      paddingX={{ base: "none", md: "s" }}
    >
      <Heading
        level="h2"
        fontSize={{
          base: "mobile.heading.heading3",
          md: "desktop.heading.heading2",
        }}
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom={{ base: "l", md: "xxl" }}
      >
        Have more questions?
      </Heading>
      <Flex
        gap={{ base: "s", md: "l" }}
        flexDir={{ base: "column", md: "row" }}
      >
        <LandingCard
          gap={{ base: "m", md: "l" }}
          icon={<MailIcon />}
          heading={
            <Heading level="h3" size="heading4">
              Connect with an expert
            </Heading>
          }
          body={
            <Flex gap={{ base: "m", md: "l" }} flexDir="column">
              <Text fontSize="desktop.subtitle.subtitle1">
                Do you need support or have more questions about using Enhanced
                Search? Our staff will be happy to guide you.
              </Text>
              <Link
                to={ASK_NYPL}
                hasVisitedState={false}
                isUnderlined={false}
                color="section.research.secondary"
                fontWeight="bold"
                display="flex"
                alignItems="center"
                justifyContent={{ base: "center", md: "left" }}
                gap="xxs"
                _hover={{
                  color: "section.research.primary",
                  path: { stroke: "#00838A" },
                }}
              >
                Contact us <ArrowIcon direction="right" color="#006166" />
              </Link>
            </Flex>
          }
        />
        <LandingCard
          gap="l"
          icon={<LightbulbIcon />}
          heading={
            <Heading level="h3" size="heading4">
              Learn more about this project
            </Heading>
          }
          body={
            <Flex gap={{ base: "m", md: "l" }} flexDir="column">
              <Text fontSize="desktop.subtitle.subtitle1">
                Are you interested in finding out more about our mission,
                collection, models, data pipelines, and technologies?
              </Text>
              <Link
                to="/learn-more"
                aria-label="Learn more about the project"
                hasVisitedState={false}
                isUnderlined={false}
                color="section.research.secondary"
                fontWeight="bold"
                display="flex"
                alignItems="center"
                justifyContent={{ base: "center", md: "left" }}
                gap="xxs"
                _hover={{
                  color: "section.research.primary",
                  path: { stroke: "#00838A" },
                }}
              >
                <Text>Learn more</Text>
                <ArrowIcon direction="right" color="#006166" />
              </Link>
            </Flex>
          }
        />
      </Flex>
      <LandingButtons
        heroSectionRef={heroSectionRef}
        textInputRef={textInputRef}
      />
    </SectionContainer>
  );
};

HelpSection.displayName = "HelpSection";

export default HelpSection;
