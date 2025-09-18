import React from "react";
import {
    Box,
    Text,
    Icon,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

const ResearchAssistantNav: React.FC = () => {
    return (
        <Box borderBottom="1px solid" borderColor="section.research.secondary">
            <Box
                height="l"
                paddingY="xs"
                display="flex"
                maxWidth="1280px"
                alignItems="center"
                margin="auto"
                gap="s"
            >
                <Box
                    display="flex"
                    alignItems="center"
                    paddingX="s"
                    paddingY="xxs"
                    backgroundColor="section.research.primary-05"
                    borderRadius="6px"
                >
                    <ResearchAssistantIcon />
                    <Text
                        size="body1"
                        color="section.research.secondary"
                        isBold
                    >
                        Virtual Research Assistant
                    </Text>
                </Box>
                <Box display="flex" alignItems="center">
                    <Icon name="search" align="left" size="large" />
                    <Text>Keyword search</Text>
                </Box>
            </Box>
        </Box>

    );
};

export default ResearchAssistantNav;
