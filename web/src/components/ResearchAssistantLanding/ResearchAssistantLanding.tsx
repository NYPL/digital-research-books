import { Box, TextInputRefType } from "@nypl/design-system-react-components";
import React, { useRef } from "react";
import VRALayout from "../VRALayout/VRALayout";
import HowItWorksSection from "./AccessSection/HowItWorksSection";
import FaqSection from "./FaqSection";
import FeaturesSection from "./FeaturesSection/FeaturesSection";
import HelpSection from "./HelpSection/HelpSection";
import HeroSection from "./HeroSection";
import PrinciplesSection from "./PrinciplesSection/PrinciplesSection";
import QuoteSection from "./QuoteSection";

const ResearchAssistantLanding: React.FC = () => {
  const heroSectionRef = useRef<HTMLDivElement>(null);
  const featuresSectionRef = useRef<HTMLDivElement>(null);
  const textInputRef = useRef<TextInputRefType>(null);

  return (
    <VRALayout
      activePage="vra"
      breadcrumbsData={[
        {
          url: "/research-assistant",
          text: "Enhanced Search (beta)",
        },
      ]}
    >
      <Box display="flex" flexDir="column">
        <HeroSection
          ref={heroSectionRef}
          featuresSectionRef={featuresSectionRef}
          textInputRef={textInputRef}
        />
        <FeaturesSection
          ref={featuresSectionRef}
          heroSectionRef={heroSectionRef}
          textInputRef={textInputRef}
        />
        <HowItWorksSection />
        <QuoteSection />
        <PrinciplesSection
          heroSectionRef={heroSectionRef}
          textInputRef={textInputRef}
        />
        <FaqSection />
        <HelpSection
          heroSectionRef={heroSectionRef}
          textInputRef={textInputRef}
        />
      </Box>
    </VRALayout>
  );
};

export default ResearchAssistantLanding;
