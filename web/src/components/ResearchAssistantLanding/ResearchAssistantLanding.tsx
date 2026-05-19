import { Box, TextInputRefType } from "@nypl/design-system-react-components";
import React, { useRef } from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import EnhancedSearchHero from "../EnhancedSearchHero/EnhancedSearchHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
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
    <>
      <DrbBreakout
        breadcrumbsData={[
          { url: "/research-assistant", text: "Enhanced Search (beta)" },
        ]}
      >
        <EnhancedSearchHero />
        <ResearchAssistantNav activePage="vra" />
      </DrbBreakout>
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
    </>
  );
};

export default ResearchAssistantLanding;
