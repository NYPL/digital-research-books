import React from "react";
import {
    Box,
    Flex,
    Template,
    TemplateBreakout,
    TemplateContent,
    TemplateFull,
    TemplateMain,
    Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "../ResearchAssistant/ResearchAssistantNav";
import KeywordSearchForm from "../KeywordSearchForm/KeywordSearchForm";

const KeywordSearchLanding: React.FC = () => {
    const breakoutElement = (
        <DrbBreakout
            breadcrumbsData={[
                { url: "/keyword-search", text: "Keyword Search" },
            ]}
        >
            <DrbHero />
            <ResearchAssistantNav activePage="keyword" />
        </DrbBreakout>
    );

    const fullElement = (
        <KeywordSearchForm />
    )

    const contentElement = (
        <Flex flexDir="column" gap="s" bgColor="ui.bg.default">
            <Box flex="1">
                <Text size="body1">Start searching to see results from over 1 million 
scholarly e-books in the public domain</Text>
            </Box>
        </Flex>
    );

    return (
        <Template>
            <TemplateBreakout>{breakoutElement}</TemplateBreakout>
            <TemplateMain paddingBottom="l">
                <TemplateFull>{fullElement}</TemplateFull>
                <TemplateContent>{contentElement}</TemplateContent>
            </TemplateMain>
        </Template>
    );
};

export default KeywordSearchLanding;
