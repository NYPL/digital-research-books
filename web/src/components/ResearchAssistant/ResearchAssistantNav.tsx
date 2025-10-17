import React from "react";
import { Icon, SubNav, SubNavLink } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import { PageType } from "~/src/types/ResearchAssistant";

interface ResearchAssistantNavProps {
    activePage: PageType;
}

const ResearchAssistantNav: React.FC<ResearchAssistantNavProps> = ({
    activePage,
}) => {
    const token = localStorage.getItem("authToken");

    return (
        <SubNav
            actionBackgroundColor="section.research.primary-05"
            highlightColor="section.research.secondary"
            primaryActions={
                <>
                    <SubNavLink
                        href="/research-assistant-landing"
                        isSelected={activePage === "vra"}
                        id="subnav-vra"
                    >
                        <ResearchAssistantIcon />
                        Virtual Research Assistant
                    </SubNavLink>
                    <SubNavLink
                        href="/keyword-search-landing"
                        isSelected={activePage === "keyword"}
                        id="subnav-keyword-search"
                    >
                        <Icon
                            color="section.research.secondary"
                            name="search"
                            align="left"
                            size="medium"
                        />
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
                    <SubNavLink
                        isOutlined
                        href={token ? "#account" : "/research-assistant-login"}
                        id="subnav-my-account"
                    >
                        <Icon name="actionIdentityFilled" size="medium" />
                        {token ? "My account" : "Login"}
                    </SubNavLink>
                </>
            }
            padding="0"
        />
    );
};

export default ResearchAssistantNav;
