import { Flex, Heading, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import SectionContainer from "../../ResearchAssistantLanding/SectionContainer";
import MissionDiagram from "./MissionDiagram";
import NumberCircle from "./NumberCircle";

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
        display="flex"
        flexDirection="column"
        margin="0 auto"
        width="100%"
        tabIndex={-1}
        paddingX="s"
        ref={ref}
      >
        <Flex flexDir="column" justifyContent="center" alignItems="center">
          <Heading
            level="h2"
            fontSize={{
              base: "mobile.heading.heading3",
              md: "desktop.heading.heading2",
            }}
            fontFamily="Domine"
            fontWeight="bold"
            marginBottom={{ base: "l", md: "xxl" }}
            textAlign="center"
          >
            Our mission
          </Heading>
          <NumberCircle number={1} />
          <Text
            fontSize={{
              base: "mobile.heading.heading5",
              md: "desktop.heading.heading5",
            }}
            color="ui.gray.dark"
            fontWeight="semibold"
            marginBottom="l"
            maxWidth={{ base: "485px", md: "none" }}
            textAlign="center"
          >
            To make Digitized Research Books available to anyone, anywhere, for
            free
          </Text>
          <NumberCircle number={2} />
          <Text
            fontSize={{
              base: "mobile.heading.heading5",
              md: "desktop.heading.heading5",
            }}
            color="ui.gray.dark"
            fontWeight="semibold"
            marginBottom="l"
            maxWidth={{ base: "485px", md: "none" }}
            textAlign="center"
          >
            To leverage AI in making this collection highly discoverable and
            accessible
          </Text>
          <NumberCircle number={3} />
          <Text
            fontSize={{
              base: "mobile.heading.heading5",
              md: "desktop.heading.heading5",
            }}
            color="ui.gray.dark"
            fontWeight="semibold"
            marginBottom={{ base: "none", md: "s" }}
            maxWidth={{ base: "485px", md: "none" }}
            textAlign="center"
          >
            To steward this project as ethically and responsibly as possible
          </Text>
        </Flex>

        <MissionDiagram />
        <Text color="ui.gray.x-dark" isItalic textAlign="center">
          Enhanced Search uses AI to make Digitized Research Books accessible to
          all
        </Text>
      </SectionContainer>
    );
  }
);

MissionSection.displayName = "MissionSection";

export default MissionSection;
