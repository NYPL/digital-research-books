import React from "react";
import {
    Flex,
    Icon,
    Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import KeywordSearchForm from "../KeywordSearchForm/KeywordSearchForm";

const KeywordSearchLanding: React.FC = () => {
    return (
        <>
            <DrbBreakout
                breadcrumbsData={[{ url: "/keyword-search", text: "Keyword Search" }]}
            >
                <DrbHero />
                <ResearchAssistantNav activePage="keyword" />
            </DrbBreakout>
            <KeywordSearchForm />
            <Flex gap="s" bgColor="ui.bg.default" alignItems="center">
                <Flex
                    alignItems="center"
                    flex="1"
                    flexDir="column"
                    gap="xs"
                    height="100%"
                    margin="0 auto"
                    maxWidth="1280px"
                    padding="xxxl"
                >
                    <Icon
                        color="section.research.secondary"
                        name="search"
                        size="xlarge"
                    />
                    <Text
                        color="section.research.secondary"
                        fontSize="1.25rem"
                        fontWeight="semibold"
                        textAlign="center"
                    >
                        Start searching to see results from over 1 million <br />
                        scholarly e-books in the public domain
                    </Text>
                </Flex>
            </Flex>
        </>
    );
};

export default KeywordSearchLanding;
