import { Flex, Heading, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import Link from "../../Link/Link";
import ArrowIcon from "../../ResearchAssistant/icons/ArrowIcon";
import LandingButtons from "../LandingButtons";
import LandingCard from "../LandingCard";
import SectionContainer from "../SectionContainer";
import LightbulbIcon from "./LightbulbIcon";
import MailIcon from "./MailIcon";

interface HelpSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const HelpSection: React.ForwardRefExoticComponent<
  HelpSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, HelpSectionProps>(({ heroSectionRef }, ref) => {
  return (
    <SectionContainer
      backgroundImage={`
        radial-gradient(circle, var(--nypl-colors-section-research-primary-10) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      alignItems="center"
      ref={ref}
    >
      <Heading
        level="h2"
        size="heading2"
        fontFamily="Domine"
        fontWeight="bold"
        marginBottom="xxl"
      >
        Have more questions?
      </Heading>
      <Flex gap="l">
        <LandingCard
          gap="l"
          icon={<MailIcon />}
          heading={
            <Heading level="h3" size="heading4">
              Connect with an expert
            </Heading>
          }
          body={
            <Flex gap="l" flexDir="column">
              <Text fontSize="desktop.subtitle.subtitle1">
                Do you need hands-on support or have other questions about using
                the Virtual Research Assistant? Our staff will be happy to guide
                you.
              </Text>
              <Link
                to="https://www.nypl.org/get-help/contact-us"
                isUnderlined={false}
                display="flex"
                alignItems="center"
                gap="xxs"
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
            <Flex gap="l" flexDir="column">
              <Text fontSize="desktop.subtitle.subtitle1">
                Are you interested in finding out more about our mission,
                corpus, models, data pipelines, or technologies? Read about them
                here.
              </Text>
              <Link
                to="#"
                isUnderlined={false}
                display="flex"
                alignItems="center"
                gap="xxs"
              >
                <Text>Learn more</Text>
                <ArrowIcon direction="right" color="#006166" />
              </Link>
            </Flex>
          }
        />
      </Flex>
      <LandingButtons heroSectionRef={heroSectionRef} />
    </SectionContainer>
  );
});

HelpSection.displayName = "HelpSection";

export default HelpSection;
