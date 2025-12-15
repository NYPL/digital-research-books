import React from "react";
import { Flex, Heading, Icon } from "@nypl/design-system-react-components";

interface EmptySearchPromptProps {
    message?: string;
}

const EmptySearchPrompt: React.FC<EmptySearchPromptProps> = ({
    message = "Start searching to see results from over 1 million scholarly e-books in the public domain",
}) => (
    <Flex gap="s" bgColor="ui.bg.default" alignItems="center" marginTop="xxl">
        <Flex
            alignItems="center"
            flex="1"
            flexDir="column"
            gap="xs"
            height="100%"
            margin="0 auto"
            maxWidth="1280px"
        >
            <Icon color="section.research.secondary" name="search" size="xlarge" />
            <Heading
                color="section.research.secondary"
                size="heading6"
                textAlign="center"
            >
                {message}
            </Heading>
        </Flex>
    </Flex>
);

export default EmptySearchPrompt;
