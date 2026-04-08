import { FeedbackBoxViewType } from "@nypl/design-system-react-components";
import React, { useContext, useEffect, useState } from "react";
import {
  BUTTON_DESCRIPTION_TEXT,
  CONFIRMATION_TEXT,
  ERROR_DESCRIPTION_TEXT,
  ERROR_NOTIFICATION_TEXT,
} from "~/src/constants/feedback";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { submitVRAFeedback } from "~/src/lib/api/FeedbackApi";

interface VRAFeedbackProps {
  location: string;
}

const VRAFeedback: React.FC<VRAFeedbackProps> = ({ location }) => {
  const [view, setView] = useState<FeedbackBoxViewType>("form");
  const {
    FeedbackBox,
    isOpen,
    onOpen,
    onClose,
    isError,
    notificationText,
    statusCode,
    setIsError,
    setNotificationText,
    descriptionText,
    setDescriptionText,
    thumbValue,
    sessionId,
  } = useContext(FeedbackContext);

  useEffect(() => {
    if (isError) {
      setDescriptionText(ERROR_DESCRIPTION_TEXT);
      setNotificationText(ERROR_NOTIFICATION_TEXT);
    } else {
      setDescriptionText(BUTTON_DESCRIPTION_TEXT);
    }
  }, [isError, setNotificationText]);

  const onCloseAndReset = () => {
    if (isError) setIsError(false);
    if (notificationText) setNotificationText(null);
    setDescriptionText(BUTTON_DESCRIPTION_TEXT);
    onClose();
    setView("form");
  };

  const handleFeedbackSubmit = (
    values: React.ComponentProps<typeof FeedbackBox>["onSubmit"]
  ) => {
    submitVRAFeedback({
      feedback: isError
        ? `Error Code: ${statusCode ?? "Unknown"} - ${values.comment}`
        : values.comment,
      category: isError ? "Bug" : values.category,
      url: location,
      email: values.email,
      sessionId: sessionId ?? "",
      thumbState: thumbValue ?? "",
    })
      .then((res) => {
        if (res.ok) setView("confirmation");
      })
      .catch((err) => {
        console.error(err);
        setView("error");
      });
    setView("confirmation");
  };

  return (
    <FeedbackBox
      showCategoryField={!isError}
      showEmailField={isError}
      isOpen={isOpen}
      onClose={onCloseAndReset}
      onOpen={onOpen}
      onSubmit={handleFeedbackSubmit}
      confirmationText={CONFIRMATION_TEXT}
      descriptionText={descriptionText}
      notificationText={notificationText}
      id="feedbackBox-id"
      title="Help and feedback"
      view={view}
    />
  );
};

export default VRAFeedback;
