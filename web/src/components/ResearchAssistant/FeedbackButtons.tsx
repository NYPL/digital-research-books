import { Button, Flex } from "@nypl/design-system-react-components";
import { useContext, useState } from "react";
import { THUMB_DESCIPTION_TEXT } from "~/src/constants/feedback";
import { FeedbackContext } from "~/src/context/FeedbackContext";
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
      fill: "#9DDEE3",
    },
  },
};

const FeedbackButtons = () => {
  const [thumbValue, setThumbValueState] = useState<FeedbackState>(null);
  const { setThumbValue, onOpen, setDescriptionText } = useContext(
    FeedbackContext
  );

  const handleThumbsUp = () => {
    const newThumbValue = thumbValue === "up" ? null : "up";

    if (thumbValue === null) {
      onOpen();
      setDescriptionText(THUMB_DESCIPTION_TEXT);
    }

    setThumbValueState(newThumbValue);
    setThumbValue(newThumbValue);
  };

  const handleThumbsDown = () => {
    const newThumbValue = thumbValue === "down" ? null : "down";

    if (thumbValue === null) {
      onOpen();
      setDescriptionText(THUMB_DESCIPTION_TEXT);
    }

    setThumbValueState(newThumbValue);
    setThumbValue(newThumbValue);
  };

  return (
    <Flex>
      <Button
        id="thumbs-up-button"
        variant="text"
        aria-label="Thumbs up"
        aria-pressed={thumbValue === "up"}
        padding="xs"
        minWidth="18px"
        onClick={handleThumbsUp}
        isDisabled={thumbValue === "down"}
        sx={feedbackButtonStyles}
      >
        <ThumbsUpIcon isDisabled={thumbValue === "down"} />
      </Button>
      <Button
        id="thumbs-down-button"
        variant="text"
        aria-label="Thumbs down"
        aria-pressed={thumbValue === "down"}
        padding="xs"
        minWidth="18px"
        onClick={handleThumbsDown}
        isDisabled={thumbValue === "up"}
        sx={feedbackButtonStyles}
      >
        <ThumbsDownIcon isDisabled={thumbValue === "up"} />
      </Button>
    </Flex>
  );
};

export default FeedbackButtons;
