import React, { useRef } from "react";
import { Box } from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import HeroSection from "./HeroSection";
import FeaturesSection from "./FeaturesSection";
import AccessSection from "./AccessSection";
import FocusedResearchSection from "./FocusedResearchSection";
import FaqSection from "./FaqSection";
import HelpSection from "./HelpSection";

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
                <FeaturesSection />
                <AccessSection />
                <FocusedResearchSection />
                <FaqSection />
                <HelpSection ref={helpSectionRef} heroSectionRef={heroSectionRef} />
            </Box>
        </>
    );
};

export default ResearchAssistantLanding;
