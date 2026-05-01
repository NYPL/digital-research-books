import { Heading, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import SectionContainer from "../SectionContainer";
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
        ref={ref}
      >
        <Heading
          level="h2"
          size="heading2"
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xxl"
        >
          Our mission
        </Heading>
        <NumberCircle number={1} />
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="l"
        >
          To make Digitized Research Books available to anyone, anywhere, for
          free
        </Text>
        <NumberCircle number={2} />

        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="l"
        >
          To make this collection highly discoverable and accessible by
          leveraging AI
        </Text>
        <NumberCircle number={3} />

        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="s"
        >
          To steward this project as ethically and responsibly as possible
        </Text>
        <MissionDiagram />
        <Text color="ui.gray.x-dark" isItalic={true}>
          Enhanced Search uses AI to make Digitized Research Books accessible to
          all
        </Text>
      </SectionContainer>
    );
  }
);

MissionSection.displayName = "MissionSection";

export default MissionSection;
