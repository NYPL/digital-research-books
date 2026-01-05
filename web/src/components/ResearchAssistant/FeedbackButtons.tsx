import { Button, Flex } from "@nypl/design-system-react-components";
import { useState } from "react";
import { FeedbackState } from "~/src/types/ResearchAssistant";
import ThumbsDownIcon from "./icons/ThumbsDownIcon";
import ThumbsUpIcon from "./icons/ThumbsUpIcon";

const feedbackButtonStyles = {
  "&:hover:not(:disabled)": {
    bgColor: "transparent",
    svg: {
      fill: "section.research.primary-10",
    },
  },
  "&[aria-pressed='true']": {
    svg: {
      fill: "section.research.primary",
    },
  },
};

const FeedbackButtons = () => {
  const [feedbackState, setFeedbackState] = useState<FeedbackState>(null);

  const handleThumbsUp = () => {
    setFeedbackState(feedbackState === "up" ? null : "up");
  };

  const handleThumbsDown = () => {
    setFeedbackState(feedbackState === "down" ? null : "down");
  };

  return (
    <Flex>
      <Button
        id="thumbs-up-button"
        variant="text"
        aria-label="Thumbs up"
        aria-pressed={feedbackState === "up"}
        padding="xs"
        minWidth="18px"
        onClick={handleThumbsUp}
        isDisabled={feedbackState === "down"}
        sx={feedbackButtonStyles}
      >
        <ThumbsUpIcon isDisabled={feedbackState === "down"} />
      </Button>
      <Button
        id="thumbs-down-button"
        variant="text"
        aria-label="Thumbs down"
        aria-pressed={feedbackState === "down"}
        padding="xs"
        minWidth="18px"
        onClick={handleThumbsDown}
        isDisabled={feedbackState === "up"}
        sx={feedbackButtonStyles}
      >
        <ThumbsDownIcon isDisabled={feedbackState === "up"} />
      </Button>
    </Flex>
  );
};

export default FeedbackButtons;
