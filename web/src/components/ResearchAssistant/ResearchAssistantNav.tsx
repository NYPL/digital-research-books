import React from "react";
import {
    Icon,
    SubNav,
    SubNavLink,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

interface ResearchAssistantNavProps {
  activePage: "vra" | "keyword";
}

const ResearchAssistantNav: React.FC<ResearchAssistantNavProps> = ({ activePage}) => {
    return (
        <SubNav
            actionBackgroundColor="section.research.primary-05"
            highlightColor="section.research.secondary"
            primaryActions={
                <>
                    <SubNavLink href="/research-assistant-landing" isSelected={activePage === "vra"} id="subnav-vra">
                        <ResearchAssistantIcon />
                        Virtual Research Assistant
                    </SubNavLink>
                    <SubNavLink href="/keyword-search-landing" isSelected={activePage === "keyword"} id="subnav-keyword-search">
                        <Icon name="search" align="left" size="medium" />
                        Keyword search
                    </SubNavLink>
                </>
            }
            secondaryActions={
                <>
                    {/* TODO: add real links to help and account pages */}
                    <SubNavLink href="#help" id="subnav-help">
                        Get help
                    </SubNavLink>
                    <SubNavLink isOutlined href="#account" id="subnav-my-account">
                        <Icon name="actionIdentityFilled" size="medium" />
                        My account
                    </SubNavLink>
                </>
            }
        />
    );
};

export default ResearchAssistantNav;
