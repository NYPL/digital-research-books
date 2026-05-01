import { Box } from "@chakra-ui/react";
import React, { useRef } from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import { DrbHero } from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import CollectionSection from "./CollectionSection";
import HeroSection from "./HeroSection/HeroSection";
import MissionSection from "./MissionSection/MissionSection";
import ModelsSection from "./ModelsSection/ModelsSection";
import TechnologySection from "./TechnologySection/TechnologySection";

const LearnMore: React.FC = () => {
  const heroSectionRef = useRef<HTMLDivElement>(null);
  const missionSectionRef = useRef<HTMLDivElement>(null);

  return (
    <>
      <DrbBreakout
        breadcrumbsData={[
          { url: "/research-assistant", text: "Virtual Research Assistant" },
        ]}
      >
        <DrbHero />
        <ResearchAssistantNav />
      </DrbBreakout>
      <Box display="flex" flexDir="column">
        <HeroSection
          ref={heroSectionRef}
          missionSectionRef={missionSectionRef}
        ></HeroSection>
        <MissionSection
          ref={missionSectionRef}
          heroSectionRef={heroSectionRef}
        ></MissionSection>
        <CollectionSection></CollectionSection>
        <TechnologySection></TechnologySection>
        <ModelsSection heroSectionRef={heroSectionRef}></ModelsSection>
      </Box>
    </>
  );
};

export default LearnMore;
