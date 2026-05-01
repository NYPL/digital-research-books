import React from "react";
import { PageType } from "~/src/types/ResearchAssistant";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
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
  <>
    <DrbBreakout breadcrumbsData={breadcrumbsData}>
      <DrbHero />
      <ResearchAssistantNav activePage={activePage} />
      <VRAPopupSurvey />
    </DrbBreakout>
    {children}
  </>
);

export default VRALayout;
