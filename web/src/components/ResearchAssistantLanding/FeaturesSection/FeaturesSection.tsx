import { Flex, Heading, Text } from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import { FEATURES } from "~/src/constants/researchAssistant";
import LandingButtons from "../LandingButtons";
import SectionContainer from "../SectionContainer";
import FeatureCard from "./FeatureCard";

interface FeaturesSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const FeaturesSection: React.ForwardRefExoticComponent<
  FeaturesSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, FeaturesSectionProps>(
  ({ heroSectionRef }, ref) => {
    return (
      <SectionContainer
        backgroundColor="#FAFDFD"
        borderTop="1px solid"
        borderColor="section.research.primary-10"
        color="ui.typography.body"
        ref={ref}
      >
        <Heading
          level="h2"
          size="heading2"
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xs"
        >
          What can the Assistant help you do?
        </Heading>
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="xxl"
        >
          A collaborative tool designed to enhance and support your research
        </Text>
        <Flex flexDir="column" gap="l">
          {FEATURES.map((feature) => (
            <FeatureCard
              key={feature.featureName}
              featureName={feature.featureName}
              title={feature.title}
              description={feature.description}
              imageSrc={feature.imageSrc}
              imageAlt={feature.imageAlt}
            />
          ))}
        </Flex>
        <LandingButtons heroSectionRef={heroSectionRef} />
      </SectionContainer>
    );
  }
);

FeaturesSection.displayName = "FeaturesSection";

export default FeaturesSection;
