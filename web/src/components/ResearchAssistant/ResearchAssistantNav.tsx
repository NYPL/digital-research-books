import React from "react";
import { Icon, SubNav, SubNavLink } from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

const ResearchAssistantNav: React.FC = () => {
    const token = localStorage.getItem("authToken");

    return (
        <SubNav
            actionBackgroundColor="section.research.primary-05"
            highlightColor="section.research.secondary"
            primaryActions={
                <>
                    <SubNavLink
                        href="/research-assistant-landing"
                        isSelected
                        id="subnav-vra"
                    >
                        <ResearchAssistantIcon />
                        Virtual Research Assistant
                    </SubNavLink>
                    <SubNavLink href="/" id="subnav-keyword-search">
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
        />
    );
};

export default ResearchAssistantNav;
