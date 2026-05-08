import { Button, Flex, Tooltip } from "@nypl/design-system-react-components";
import { useContext, useState } from "react";
import { THUMB_DESCIPTION_TEXT } from "~/src/constants/feedback";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import { FeedbackState } from "~/src/types/ResearchAssistant";
import ThumbsDownIcon from "./icons/ThumbsDownIcon";
import ThumbsUpIcon from "./icons/ThumbsUpIcon";

const feedbackButtonStyles = {
  "&:hover:not(:disabled)": {
    bgColor: "section.research.primary-10",
  },
  "&[aria-pressed='true']": {
    svg: {
      fill: "#9DDEE3",
    },
  },
};

interface FeedbackButtonsProps {
  label: string;
}

const FeedbackButtons: React.FC<FeedbackButtonsProps> = ({ label }) => {
  const [thumbValue, setThumbValueState] = useState<FeedbackState>(null);
  const { setThumbValue, onOpen, setDescriptionText } = useContext(
    FeedbackContext
  );

  const handleThumbsUp = () => {
    const newThumbValue = thumbValue === "up" ? null : "up";

    if (thumbValue === null) {
      // GTM Tagging: feedback_collection_click, thumbs_up_button
      trackEvent({
        event: "feedback_collection_click",
        interaction: "Click",
        element_id: "thumbs-up-button",
      });
      onOpen();
      setDescriptionText(THUMB_DESCIPTION_TEXT);
    }
    setThumbValueState(newThumbValue);
    setThumbValue(newThumbValue);
  };

  const handleThumbsDown = () => {
    const newThumbValue = thumbValue === "down" ? null : "down";

    if (thumbValue === null) {
      // GTM Tagging: feedback_collection_click, thumbs_down_button
      trackEvent({
        event: "feedback_collection_click",
        interaction: "Click",
        element_id: "thumbs-down-button",
      });
      onOpen();
      setDescriptionText(THUMB_DESCIPTION_TEXT);
    }

    setThumbValueState(newThumbValue);
    setThumbValue(newThumbValue);
  };

  return (
    <Flex>
      <Tooltip content="Good response">
        <Button
          id="thumbs-up-button"
          variant="text"
          aria-label={`${label} Good response`}
          aria-pressed={thumbValue === "up"}
          padding="xs"
          borderRadius="100px"
          height="32px"
          width="32px"
          minHeight="32px"
          minWidth="32px"
          onClick={handleThumbsUp}
          isDisabled={thumbValue === "down"}
          sx={feedbackButtonStyles}
        >
          <ThumbsUpIcon isDisabled={thumbValue === "down"} />
        </Button>
      </Tooltip>
      <Tooltip content="Bad response">
        <Button
          id="thumbs-down-button"
          variant="text"
          aria-label={`${label} Bad response`}
          aria-pressed={thumbValue === "down"}
          padding="xs"
          borderRadius="100px"
          height="32px"
          width="32px"
          minHeight="32px"
          minWidth="32px"
          onClick={handleThumbsDown}
          isDisabled={thumbValue === "up"}
          sx={feedbackButtonStyles}
        >
          <ThumbsDownIcon isDisabled={thumbValue === "up"} />
        </Button>
      </Tooltip>
    </Flex>
  );
};

export default FeedbackButtons;
