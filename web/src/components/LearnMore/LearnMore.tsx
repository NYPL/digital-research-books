import { Box } from "@chakra-ui/react";
import React, { useRef } from "react";
import { breadcrumbTitles } from "~/src/constants/labels";
import VRALayout from "../VRALayout/VRALayout";
import CollectionSection from "./CollectionSection";
import HeroSection from "./HeroSection/HeroSection";
import MissionSection from "./MissionSection/MissionSection";
import ModelsSection from "./ModelsSection/ModelsSection";
import TechnologySection from "./TechnologySection/TechnologySection";

const LearnMore: React.FC = () => {
  const heroSectionRef = useRef<HTMLDivElement>(null);
  const missionSectionRef = useRef<HTMLDivElement>(null);

  return (
    <VRALayout
      activePage="vra"
      breadcrumbsData={[
        {
          url: "/learn-more",
          text: breadcrumbTitles.learnMore,
        },
      ]}
    >
      <Box display="flex" flexDir="column">
        <HeroSection
          ref={heroSectionRef}
          missionSectionRef={missionSectionRef}
        />
        <MissionSection
          ref={missionSectionRef}
          heroSectionRef={heroSectionRef}
        />
        <CollectionSection />
        <TechnologySection />
        <ModelsSection heroSectionRef={heroSectionRef} />
      </Box>
    </VRALayout>
  );
};

export default LearnMore;
