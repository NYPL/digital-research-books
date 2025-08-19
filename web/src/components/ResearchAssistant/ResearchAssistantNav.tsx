import React from "react";
import {
    Icon,
    SubNav,
    SubNavButton,
    SubNavLink,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

const ResearchAssistantNav: React.FC = () => {
    return (
        <SubNav
            actionBackgroundColor="section.research.primary-05"
            highlightColor="section.research.secondary"
            primaryActions={
                <>
                    <SubNavButton onClick={() => { }} isSelected id="subnav-vra">
                        <ResearchAssistantIcon />
                        Virtual Research Assistant
                    </SubNavButton>
                    <SubNavLink href="/" id="subnav-keyword-search">
                        <Icon name="search" align="left" size="medium" />
                        Keyword search
                    </SubNavLink>
                </>
            }
            secondaryActions={
                <>
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
