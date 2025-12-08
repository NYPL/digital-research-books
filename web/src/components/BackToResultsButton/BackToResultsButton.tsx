import { Button, Flex } from "@nypl/design-system-react-components";
import React from "react";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";

interface BackToResultsButtonProps {
    handleBackToResults: () => void;
}

const BackToResultsButton: React.FC<BackToResultsButtonProps> = ({
    handleBackToResults,
}) => {
    return (
        <Button
            variant="text"
            id="back-button"
            color="section.research.secondary"
            onClick={handleBackToResults}
        >
            <Flex alignItems="center" gap="xs">
                <ArrowIcon direction="left" color="#006166" />
                <span>Back to results</span>
            </Flex>
        </Button>
    );
};

export default BackToResultsButton;
