import React from "react";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import { PageType } from "~/src/types/ResearchAssistant";
import { ResultPageProvider } from "~/src/context/ResultPageContext";

interface VRALayoutProps {
    activePage: PageType;
    breadcrumbsData?: { url: string; text: string }[];
    onReadOnline?: (linkId: number) => void;
    children: React.ReactNode;
}

const VRALayout: React.FC<VRALayoutProps> = ({
    activePage,
    breadcrumbsData = [],
    onReadOnline = () => { },
    children,
}) => (
    <ResultPageProvider
        value={{
            onReadOnline: onReadOnline,
            page: activePage,
        }}
    >
        <DrbBreakout breadcrumbsData={breadcrumbsData}>
            <DrbHero />
            <ResearchAssistantNav activePage={activePage} />
        </DrbBreakout>
        {children}
    </ResultPageProvider>
);

export default VRALayout;
