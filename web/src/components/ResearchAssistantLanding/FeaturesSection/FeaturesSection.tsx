import {
  Flex,
  Heading,
  Text,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import { forwardRef } from "react";
import { FEATURES } from "~/src/constants/researchAssistant";
import LandingButtons from "../LandingButtons";
import SectionContainer from "../SectionContainer";
import FeatureCard from "./FeatureCard";

interface FeaturesSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
  textInputRef: React.RefObject<TextInputRefType>;
}

const FeaturesSection: React.ForwardRefExoticComponent<
  FeaturesSectionProps & React.RefAttributes<HTMLDivElement>
> = forwardRef<HTMLDivElement, FeaturesSectionProps>(
  ({ heroSectionRef, textInputRef }, ref) => {
    return (
      <SectionContainer
        backgroundColor="#FAFDFD"
        borderTop="1px solid"
        borderColor="section.research.primary-10"
        color="ui.typography.body"
        tabIndex={-1}
        ref={ref}
        paddingX={{ base: "none", md: "xs" }}
      >
        <Heading
          level="h2"
          fontSize={{
            base: "mobile.heading.heading3",
            md: "desktop.heading.heading2",
          }}
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xs"
          paddingX={{ base: "s", md: "none" }}
        >
          What can Enhanced Search help you do?
        </Heading>
        <Text
          fontSize={{
            base: "mobile.heading.heading5",
            md: "desktop.heading.heading5",
          }}
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom={{ base: "l", md: "xxl" }}
          paddingX={{ base: "s", md: "none" }}
        >
          A collaborative discovery tool designed to support your research
        </Text>
        <Flex flexDir="column" gap={{ base: "s", md: "l" }}>
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
        <LandingButtons
          heroSectionRef={heroSectionRef}
          textInputRef={textInputRef}
        />
      </SectionContainer>
    );
  }
);

FeaturesSection.displayName = "FeaturesSection";

export default FeaturesSection;
