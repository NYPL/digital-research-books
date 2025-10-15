import React from "react";
import { Flex, Heading, Icon } from "@nypl/design-system-react-components";
import KeywordSearchForm from "../KeywordSearchForm/KeywordSearchForm";

const KeywordSearchLanding: React.FC = () => {
    return (
        <>
            <KeywordSearchForm paddingBottom="l" />
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
                    <Heading
                        color="section.research.secondary"
                        size="heading6"
                        textAlign="center"
                        width="600px"
                    >
                        Start searching to see results from over 1 million scholarly e-books
                        in the public domain
                    </Heading>
                </Flex>
            </Flex>
        </>
    );
};

export default KeywordSearchLanding;
