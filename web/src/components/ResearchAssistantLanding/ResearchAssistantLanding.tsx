import { Box } from "@nypl/design-system-react-components";
import React, { useRef } from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import AccessSection from "./AccessSection/AccessSection";
import FaqSection from "./FaqSection";
import FeaturesSection from "./FeaturesSection/FeaturesSection";
import HelpSection from "./HelpSection";
import HeroSection from "./HeroSection";

const ResearchAssistantLanding: React.FC = () => {
  const heroSectionRef = useRef<HTMLDivElement>(null);
  const helpSectionRef = useRef<HTMLDivElement>(null);

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
        <HeroSection ref={heroSectionRef} helpSectionRef={helpSectionRef} />
        <FeaturesSection heroSectionRef={heroSectionRef} />
        <AccessSection heroSectionRef={heroSectionRef} />
        <FaqSection />
        <HelpSection ref={helpSectionRef} heroSectionRef={heroSectionRef} />
      </Box>
    </>
  );
};

export default ResearchAssistantLanding;
