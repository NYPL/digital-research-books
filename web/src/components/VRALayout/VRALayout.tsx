import React from "react";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import { PageType } from "~/src/types/ResearchAssistant";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import EnhancedSearchHero from "../EnhancedSearchHero/EnhancedSearchHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import VRAPopupSurvey from "../VRAPopupSurvey/VRAPopupSurvey";

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
  <ResultPageProvider value={{ page: activePage }}>
    <DrbBreakout breadcrumbsData={breadcrumbsData}>
      <EnhancedSearchHero />
      <ResearchAssistantNav activePage={activePage} />
      <VRAPopupSurvey />
    </DrbBreakout>
    {children}
  </ResultPageProvider>
);

export default VRALayout;
