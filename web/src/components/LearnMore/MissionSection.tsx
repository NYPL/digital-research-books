import { Heading, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import SectionContainer from "../ResearchAssistantLanding/SectionContainer";

interface FeaturesSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const MissionSection: React.ForwardRefExoticComponent<
  FeaturesSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, FeaturesSectionProps>(
  ({ heroSectionRef }, ref) => {
    return (
      <SectionContainer
        backgroundColor="#FAFDFD"
        borderTop="1px solid"
        borderColor="section.research.primary-10"
        color="ui.typography.body"
        tabIndex={-1}
        ref={ref}
      >
        <Heading
          level="h2"
          size="heading3"
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xs"
        >
          Our mission
        </Heading>
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="s"
        >
          To make Digitized Research Books available to anyone, anywhere, for
          free
        </Text>
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="s"
        >
          To make this collection highly discoverable and accessible by
          leveraging AI
        </Text>
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="s"
        >
          To steward this project as ethically and responsibly as possible
        </Text>
        <Text color="ui.gray.x-dark" marginBottom="m">
          Our collection of Digitized Research Books as of 1 July 2026
          accessible through Enhanced Search
        </Text>
      </SectionContainer>
    );
  }
);

MissionSection.displayName = "MissionSection";

export default MissionSection;
