import { Box, TextInputRefType } from "@nypl/design-system-react-components";
import React, { useRef } from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import AccessSection from "./AccessSection/AccessSection";
import FaqSection from "./FaqSection";
import FeaturesSection from "./FeaturesSection/FeaturesSection";
import HelpSection from "./HelpSection/HelpSection";
import HeroSection from "./HeroSection";

const ResearchAssistantLanding: React.FC = () => {
  const heroSectionRef = useRef<HTMLDivElement>(null);
  const featuresSectionRef = useRef<HTMLDivElement>(null);
  const textInputRef = useRef<TextInputRefType>(null);

  return (
    <>
      <DrbBreakout
        breadcrumbsData={[
          { url: "/research-assistant", text: "Virtual Research Assistant" },
        ]}
      >
        <DrbHero />
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
        <AccessSection
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
