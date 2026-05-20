import {
  Box,
  Flex,
  Icon,
  SubNav,
  SubNavButton,
  SubNavLink,
  Text,
} from "@nypl/design-system-react-components";
import React, { useContext, useEffect, useState } from "react";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { PageType } from "~/src/types/ResearchAssistant";
import KeywordSearchIcon from "./icons/KeywordSearchIcon";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";

interface ResearchAssistantNavProps {
  activePage?: PageType;
}

const ResearchAssistantNav: React.FC<ResearchAssistantNavProps> = ({
  activePage,
}) => {
  const [token, setToken] = useState<string | null>(null);

  const { onOpen } = useContext(FeedbackContext);

  useEffect(() => {
    if (typeof window !== "undefined") {
      setToken(localStorage.getItem("authToken"));
    }
  }, []);

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
            <Flex display="flex" alignItems="center" gap="xxs">
              <ResearchAssistantIcon
                color={activePage === "vra" ? "#006166" : "ui.black"}
                size="medium"
              />
              <>Enhanced Search</>
              <Text
                padding="2px 8px"
                bg="#F9E08E"
                borderRadius="24px"
                color="ui.black"
                fontSize="10px"
                fontWeight="semibold"
              >
                BETA
              </Text>
            </Flex>
          </SubNavLink>
          <SubNavLink
            href="/keyword-search-landing"
            isSelected={activePage === "keyword"}
            id="subnav-keyword-search"
          >
            <Box>
              <KeywordSearchIcon color="section.research.secondary" />
            </Box>
            Keyword search
          </SubNavLink>
        </>
      }
      secondaryActions={
        <>
          {/* TODO: add real links to help and account pages */}
          <SubNavButton onClick={onOpen} id="subnav-contact-us">
            Contact us
          </SubNavButton>
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
      sx={{
        a: {
          gap: "xxs",
        },
        "#keyword-search-icon": {
          fill: "none !important",
        },
        'a:not(.ds-subNav-selectedItem) [id$="-icon"]': {
          color: "ui.black",
          path: {
            stroke: "ui.black",
          },
        },
        "a:not(.ds-subNav-selectedItem) #research-assistant-icon path": {
          fill: "ui.black",
        },
      }}
    />
  );
};

export default ResearchAssistantNav;
