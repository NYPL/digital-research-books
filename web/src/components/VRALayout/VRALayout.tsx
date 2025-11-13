import React from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import { PageType } from "~/src/types/ResearchAssistant";
import { ResearchAssistantProvider } from "~/src/context/ResearchAssistantContext";

interface VRALayoutProps {
    activePage: PageType;
    breadcrumbsData?: { url: string; text: string }[];
    children: React.ReactNode;
}

const VRALayout: React.FC<VRALayoutProps> = ({
    activePage,
    breadcrumbsData = [],
    children,
}) => (
    <ResearchAssistantProvider>
        <DrbBreakout breadcrumbsData={breadcrumbsData}>
            <DrbHero />
            <ResearchAssistantNav activePage={activePage} />
        </DrbBreakout>
        {children}
    </ResearchAssistantProvider>
);

export default VRALayout;
