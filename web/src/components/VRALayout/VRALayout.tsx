import React from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import { PageType } from "~/src/types/ResearchAssistant";

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
    <>
        <DrbBreakout breadcrumbsData={breadcrumbsData}>
            <DrbHero />
            <ResearchAssistantNav activePage={activePage} />
        </DrbBreakout>
        {children}
    </>
);

export default VRALayout;
