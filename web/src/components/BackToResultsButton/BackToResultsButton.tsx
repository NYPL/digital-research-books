import { Button, Icon } from "@nypl/design-system-react-components";
import React from "react";

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
            <Icon name="arrow" iconRotation="rotate90" align="left" size="small" />
            Back to results
        </Button>
    );
};

export default BackToResultsButton;
